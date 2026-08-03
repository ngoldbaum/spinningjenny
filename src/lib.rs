use pyo3::prelude::*;

/// A faster ThreadPoolExecutor.
#[pymodule]
mod spinningjenny {
    use std::sync::{
        mpsc::{channel, Receiver},
        Mutex,
    };

    use pyo3::prelude::*;
    use rayon::{ThreadPool, ThreadPoolBuilder};

    #[pyclass]
    struct _ResultIter {
        receiver: Mutex<Receiver<PyResult<Py<PyAny>>>>,
    }

    impl _ResultIter {
        fn new(receiver: Receiver<PyResult<Py<PyAny>>>) -> Self {
            Self {
                receiver: Mutex::new(receiver),
            }
        }
    }

    #[pymethods]
    impl _ResultIter {
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
    }

    #[pymethods]
    impl ThreadPoolExecutor {
        #[new]
        fn py_new(n_threads: usize) -> PyResult<Self> {
            Ok(Self {
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

        fn map(
            &self,
            py: Python<'_>,
            func: Py<PyAny>,
            iterable: Py<PyAny>,
        ) -> PyResult<Py<_ResultIter>> {
            let (sender, receiver) = channel::<PyResult<Py<PyAny>>>();
            for value in iterable.bind(py).try_iter()? {
                let value = value?.unbind();
                let func = func.clone_ref(py);
                let sender = sender.clone();
                self.pool.spawn_fifo(move || {
                    let result = Python::attach(move |py| func.call1(py, (value,)));
                    sender.send(result).unwrap();
                });
            }
            Py::new(py, _ResultIter::new(receiver))
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
