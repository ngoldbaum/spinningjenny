use pyo3::prelude::*;

/// A faster ThreadPoolExecutor.
#[pymodule]
#[pyo3(name = "_spinningjenny")]
mod spinningjenny {
    use std::sync::{
        Mutex,
        mpsc::{Receiver, channel},
    };

    use pyo3::prelude::*;
    use rayon::{ThreadPool, ThreadPoolBuilder};

    #[pyclass]
    struct ResultIter {
        receiver: Mutex<Receiver<PyResult<Py<PyAny>>>>,
    }

    impl ResultIter {
        fn new(receiver: Receiver<PyResult<Py<PyAny>>>) -> Self {
            Self {
                receiver: Mutex::new(receiver),
            }
        }
    }

    #[pymethods]
    impl ResultIter {
        fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
            slf
        }

        fn __next__(&self, py: Python<'_>) -> Option<PyResult<Py<PyAny>>> {
            if let Ok(result) = self.receiver.lock().unwrap().try_recv() {
                return Some(result);
            }
            let receiver = &self.receiver;
            // detach before waiting to receive, lest we deadlock with the
            // interpreter
            py.detach(|| receiver.lock().unwrap().recv().ok())
        }
    }

    #[pyclass]
    struct ThreadPoolExecutor {
        pool: ThreadPool,
        /// Cached `contextvars.copy_context`:
        copy_context: Py<PyAny>,
    }

    #[pymethods]
    impl ThreadPoolExecutor {
        #[new]
        fn py_new(py: Python<'_>, n_threads: usize) -> PyResult<Self> {
            let copy_context = py.import("contextvars")?.getattr("copy_context")?.unbind();
            Ok(Self {
                copy_context,
                pool: ThreadPoolBuilder::new()
                    .num_threads(n_threads)
                    .spawn_handler(|thread| {
                        // stay detached while idle so parked workers don't
                        // deadlock with the interpreter
                        std::thread::spawn(move || Python::attach(|py| py.detach(|| thread.run())));
                        Ok(())
                    })
                    .build()
                    .expect("TODO handle error"),
            })
        }

        fn map_unordered(
            &self,
            py: Python<'_>,
            func: Py<PyAny>,
            iterable: Py<PyAny>,
        ) -> PyResult<Py<ResultIter>> {
            let (sender, receiver) = channel::<PyResult<Py<PyAny>>>();

            // Copy the current contextvars context:
            let context = self.copy_context.call0(py)?;

            for value in iterable.bind(py).try_iter()? {
                let value = value?.unbind();
                let func = func.clone_ref(py);
                let context = context.clone_ref(py);
                let sender = sender.clone();
                self.pool.spawn_fifo(move || {
                    let result = Python::attach(move |thread_py| {
                        // Can't have the same context called by multiple
                        // threads at once, so we need to copy it. Copying seems
                        // cheap, so that's OK.
                        let local_context = context.call_method0(thread_py, "copy")?;
                        // Run the function under the context:
                        local_context.call_method(thread_py, "run", (func, value), None)
                    });
                    sender.send(result).unwrap();
                });
            }
            Py::new(py, ResultIter::new(receiver))
        }

        fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
            slf
        }

        fn __exit__(
            &self,
            _exc_type: Bound<'_, PyAny>,
            _exc_value: Bound<'_, PyAny>,
            _exc_traceback: Bound<'_, PyAny>,
        ) -> bool {
            false
        }
    }
}
