use pyo3::prelude::*;

use std::sync::mpsc::{SendError, Sender, SyncSender, TrySendError};

/// One of `std::sync::mpsc::{Sender,SyncSender}`.
trait CommonSender<T>: Clone + Send {
    /// Same as underlying classes.
    fn send(&self, t: T) -> Result<(), SendError<T>>;

    /// Either same as underlying class, or always succeeds.
    fn try_send(&self, t: T) -> Result<(), TrySendError<T>>;
}

impl<T: Send> CommonSender<T> for Sender<T> {
    fn send(&self, t: T) -> Result<(), SendError<T>> {
        self.send(t)
    }

    fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        match self.send(t) {
            Ok(()) => Ok(()),
            Err(SendError(t)) => Err(TrySendError::Disconnected(t)),
        }
    }
}

impl<T: Send> CommonSender<T> for SyncSender<T> {
    fn send(&self, t: T) -> Result<(), SendError<T>> {
        self.send(t)
    }

    fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        self.try_send(t)
    }
}

/// A faster ThreadPoolExecutor.
#[pymodule]
#[pyo3(name = "_spinningjenny")]
mod spinningjenny {
    use std::{
        cell::{Cell, RefCell},
        sync::{
            Mutex,
            mpsc::{Receiver, TrySendError, channel, sync_channel},
        },
    };

    use super::CommonSender;
    use pyo3::{prelude::*, types::PyIterator};
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

    thread_local! {
        // The `contextvars` Context to run functions in.
        pub static CONTEXTVARS_CONTEXT: RefCell<Option<Py<PyAny>>> = const { RefCell::new(None) };
        // Each `map_unordered()` call in the current thread increments the
        // generation, so we can distinguish contexts between them..
        pub static GENERATION: Cell<usize> = const { Cell::new(0) };
    }

    /// Increment the generation.
    fn new_generation() -> usize {
        let result = GENERATION.get() + 1;
        GENERATION.set(result);
        result
    }

    /// Return a copy of `parent_context`, which is a `contextvars.Context`. If
    /// possible this context is retrieved and cached on a thread local.
    fn thread_local_context(
        py: Python<'_>,
        parent_context: Py<PyAny>,
        generation: usize,
    ) -> PyResult<Py<PyAny>> {
        let context = if let Some(context) = CONTEXTVARS_CONTEXT.take()
            && GENERATION.get() == generation
        {
            context
        } else {
            GENERATION.set(generation);
            parent_context.call_method0(py, "copy")?
        };
        let result = context.clone_ref(py);
        CONTEXTVARS_CONTEXT.replace(Some(context));
        Ok(result)
    }

    #[pyclass]
    struct ThreadPoolExecutor {
        pool: ThreadPool,
        n_threads: usize,
        /// Cached `contextvars.copy_context`:
        copy_context: Py<PyAny>,
    }

    impl ThreadPoolExecutor {
        /// Run all tasks in the given context, with either a ``Sender`` or
        /// ``SyncSender`` getting the results.
        fn map_unordered_for_sender<
            C: CommonSender<PyResult<Py<PyAny>>> + 'static + Send + Sync,
        >(
            &self,
            context: Py<PyAny>,
            func: Py<PyAny>,
            py_iterator: Py<PyIterator>,
            sender: C,
        ) {
            let generation = new_generation();

            let n_threads = self.n_threads;
            // Iterate over the Python iterator in the thread pool, and spawn
            // tasks there. The LIFO nature of Rayon's spawn() should ensure
            // lazy iteration over the Python iterator.
            self.pool.spawn(move || {
                let orig_sender = sender.clone();
                let result = Python::attach(move |iterating_py| {
                    for (i, value) in py_iterator.bind(iterating_py).into_iter().enumerate() {
                        let value = value?.unbind();
                        let func = func.clone_ref(iterating_py);
                        let context = context.clone_ref(iterating_py);
                        let sender = sender.clone();
                        // This will spawn within the current pool.
                        rayon::spawn_fifo(move || {
                            let result = Python::attach(move |thread_py| {
                                // Can't have the same context called by multiple
                                // threads at once, so we need a copy per thread.
                                let local_context =
                                    thread_local_context(thread_py, context, generation)?;
                                // Run the function under the context:
                                local_context.call_method(thread_py, "run", (func, value), None)
                            });
                            // We don't want to block while attached, since that
                            // can block Python GC, resulting in deadlock when
                            // buffersize is set and these threads block. And
                            // sometimes this will run inline in the iterating
                            // thread so it will still be attached.
                            if let Err(TrySendError::Full(result)) = sender.try_send(result) {
                                Python::attach(|thread_py| {
                                    thread_py.detach(|| sender.send(result).unwrap())
                                });
                            };
                        });
                        // Occasionally take a break from iterating to run some
                        // tasks in this thread, so that we don't load too many
                        // tasks into memory.
                        if i.is_multiple_of(4 * n_threads) {
                            for _ in 0..4 {
                                if rayon::yield_local() == Some(rayon::Yield::Idle) {
                                    break;
                                }
                            }
                        }
                    }
                    PyResult::Ok(())
                });
                if let Err(err) = result {
                    orig_sender.send(Err(err)).unwrap();
                }
            });
        }
    }

    #[pymethods]
    impl ThreadPoolExecutor {
        #[new]
        fn py_new(py: Python<'_>, n_threads: usize) -> PyResult<Self> {
            let copy_context = py.import("contextvars")?.getattr("copy_context")?.unbind();
            Ok(Self {
                copy_context,
                n_threads,
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

        #[pyo3(signature = (func, iterable, buffersize = None))]
        fn map_unordered(
            &self,
            py: Python<'_>,
            func: Py<PyAny>,
            iterable: Py<PyAny>,
            buffersize: Option<usize>,
        ) -> PyResult<Py<ResultIter>> {
            // Copy the current contextvars context:
            let context = self.copy_context.call0(py)?;
            // Make sure the iterable is iterable:
            let py_iterator = iterable.bind(py).try_iter()?.unbind();

            if let Some(buffersize) = buffersize {
                let (sender, receiver) = sync_channel::<PyResult<Py<PyAny>>>(buffersize);
                self.map_unordered_for_sender(context, func, py_iterator, sender);
                Py::new(py, ResultIter::new(receiver))
            } else {
                let (sender, receiver) = channel::<PyResult<Py<PyAny>>>();
                self.map_unordered_for_sender(context, func, py_iterator, sender);
                Py::new(py, ResultIter::new(receiver))
            }
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
