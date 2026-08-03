use pyo3::prelude::*;

/// A faster ThreadPoolExecutor.
#[pymodule]
mod spinningjenny {
    use std::sync::{
        Mutex,
        mpsc::{Receiver, channel},
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

        fn __next__(slf: PyRef<'_, Self>) -> Option<PyResult<Py<PyAny>>> {
            slf.receiver.lock().unwrap().recv().ok()
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
                    Python::attach(|thread_py| {
                        sender.send(func.call1(thread_py, (value,))).unwrap()
                    });
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
