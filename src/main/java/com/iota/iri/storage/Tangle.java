package com.iota.iri.storage;

import com.iota.iri.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by paul on 3/3/17 for iri.
 */
public class Tangle {
    private static final Logger log = LoggerFactory.getLogger(Tangle.class);

    private static final Tangle instance = new Tangle();
    private final List<PersistenceProvider> persistenceProviders = new ArrayList<>();
    private ExecutorService executor;

    public void addPersistenceProvider(PersistenceProvider provider) {
        this.persistenceProviders.add(provider);
    }

    public void init() throws Exception {
        executor = Executors.newCachedThreadPool();
        for(PersistenceProvider provider: this.persistenceProviders) {
            provider.init();
        }
        //new TransactionViewModel(TransactionViewModel.NULL_TRANSACTION_BYTES, new int[TransactionViewModel.TRINARY_SIZE], null).store();
    }


    public void shutdown() throws Exception {
        log.info("Shutting down Tangle Persistence Providers... ");
        executor.shutdown();
        executor.awaitTermination(6, TimeUnit.SECONDS);
        this.persistenceProviders.forEach(PersistenceProvider::shutdown);
        this.persistenceProviders.clear();
    }

    private Object loadNow(Class<?> model, Indexable index) throws Exception {
        Object out = null;
        for(PersistenceProvider provider: this.persistenceProviders) {
            if((out = provider.get(model, index)) != null) {
                break;
            }
        }
        return out;
    }

    public Future<Object> load(Class<?> model, Indexable index) {
        return executor.submit(() -> loadNow(model, index));
    }

    public Future<Boolean> save(Persistable model, Indexable index) {
        return executor.submit(() -> {
            boolean exists = false;
            for(PersistenceProvider provider: persistenceProviders) {
                if(exists) {
                    provider.save(model, index);
                } else {
                   exists = provider.save(model, index);
                }
            }
            return exists;
        });
    }

    public Future<Void> delete(Class<?> model, Indexable index) {
        return executor.submit(() -> {
            for(PersistenceProvider provider: persistenceProviders) {
                //while(!provider.isAvailable()) {}
                provider.delete(model, index);
            }
            return null;
        });
    }

    public Future<Object> getLatest(Class<?> model) {
        return executor.submit(() -> {
            Persistable latest = null;
            for(PersistenceProvider provider: persistenceProviders) {
                if (latest == null) {
                    latest = provider.latest(model);
                }
            }
            return latest;
        });
    }

    public Future<Boolean> update(Persistable model, Indexable index, String item) {
        return executor.submit(() -> {
            boolean success = false;
            for(PersistenceProvider provider: this.persistenceProviders) {
                if(success) {
                    provider.update(model, index, item);
                } else {
                    success = provider.update(model, index, item);
                }
            }
            return success;
        });
    }

    public static Tangle instance() {
        return instance;
    }

    public Future<Set<Indexable>> keysWithMissingReferences(Class<?> modelClass) {
        return executor.submit(() -> {
            Set<Indexable> output = null;
            for(PersistenceProvider provider: this.persistenceProviders) {
                output = provider.keysWithMissingReferences(modelClass);
                if(output != null && output.size() > 0) {
                    break;
                }
            }
            return output;
        });
    }

    public Future<Set<Indexable>> keysStartingWith(Class<?> modelClass, byte[] value) {
        return executor.submit(() -> {
            Set<Indexable> output = null;
            for(PersistenceProvider provider: this.persistenceProviders) {
                output = provider.keysStartingWith(modelClass, value);
                if(output.size() != 0) {
                    break;
                }
            }
            return output;
        });
    }

    public Future<Boolean> exists(Class<?> modelClass, Indexable hash) {
        return executor.submit(() -> {
            for(PersistenceProvider provider: this.persistenceProviders) {
                if(provider.exists(modelClass, hash)) return true;
            }
            return false;
        });
    }

    public Future<Boolean> maybeHas(Class<?> model, Indexable index) {
        return executor.submit(() -> {
            for(PersistenceProvider provider: this.persistenceProviders) {
                if(provider.mayExist(model, index)) return true;
            }
            return false;
        });
    }

    public Future<Long> getCount(Class<?> modelClass) {
        return executor.submit(() -> {
            long value = 0;
            for(PersistenceProvider provider: this.persistenceProviders) {
                if((value = provider.count(modelClass)) != 0) {
                    break;
                }
            }
            return value;
        });
    }

    public Future<Object> find(Class<?> model, byte[] key) {
        return executor.submit(() -> {
            Object out = null;
            for (PersistenceProvider provider : this.persistenceProviders) {
                if ((out = provider.seek(model, key)) != null) {
                    break;
                }
            }
            return out;
        });
    }

    public Future<Object> next(Class<?> model, Indexable index) {
        return executor.submit(() -> {
            Object latest = null;
            for(PersistenceProvider provider: persistenceProviders) {
                if(latest == null) {
                    latest = provider.next(model, index);
                }
            }
            return latest;

        });
    }
    public Future<Object> previous(Class<?> model, Indexable index) {
        return executor.submit(() -> {
            Object latest = null;
            for(PersistenceProvider provider: persistenceProviders) {
                if(latest == null) {
                    latest = provider.previous(model, index);
                }
            }
            return latest;

        });
    }

    public Future<Object> getFirst(Class<?> model) {
        return executor.submit(() -> {
            Object latest = null;
            for(PersistenceProvider provider: persistenceProviders) {
                if(latest == null) {
                    latest = provider.first(model);
                }
            }
            return latest;
        });
    }
}
