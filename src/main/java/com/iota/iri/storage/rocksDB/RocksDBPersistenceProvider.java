package com.iota.iri.storage.rocksDB;

import com.iota.iri.conf.Configuration;
import com.iota.iri.model.*;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.PersistenceProvider;
import com.iota.iri.utils.Serializer;
import org.apache.commons.lang3.SystemUtils;
import org.rocksdb.*;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Created by paul on 3/2/17 for iri.
 */
public class RocksDBPersistenceProvider implements PersistenceProvider {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(RocksDBPersistenceProvider.class);
    private static final int BLOOM_FILTER_BITS_PER_KEY = 10;

    private final String[] columnFamilyNames = new String[]{
            "transaction",
            "milestone",
            "stateDiff",
            "hashes",
    };

    private ColumnFamilyHandle transactionHandle;
    private ColumnFamilyHandle milestoneHandle;
    private ColumnFamilyHandle stateDiffHandle;
    private ColumnFamilyHandle hashesHandle;

    private List<ColumnFamilyHandle> transactionGetList;

    private final AtomicReference<Map<Class<?>, ColumnFamilyHandle>> classTreeMap = new AtomicReference<>();
    private final Map<Class<?>, Long> counts = new ConcurrentHashMap<>();

    private final SecureRandom seed = new SecureRandom();

    private RocksDB db;
    private DBOptions options;
    private BloomFilter bloomFilter;
    private boolean available;

    @Override
    public void init() throws Exception {
        log.info("Initializing Database Backend... ");
        initDB(
                Configuration.string(Configuration.DefaultConfSettings.DB_PATH),
                Configuration.string(Configuration.DefaultConfSettings.DB_LOG_PATH)
        );
        initClassTreeMap();
        available = true;
        log.info("RocksDB persistence provider initialized.");
    }

    @Override
    public boolean isAvailable() {
        return this.available;
    }


    private void initClassTreeMap() throws RocksDBException {
        Map<Class<?>, ColumnFamilyHandle> classMap = new HashMap<>();
        classMap.put(Transaction.class, transactionHandle);
        classMap.put(Milestone.class, milestoneHandle);
        classMap.put(StateDiff.class, stateDiffHandle);
        classMap.put(Hashes.class, hashesHandle);
        classTreeMap.set(classMap);

        counts.put(Transaction.class, getCountEstimate(Transaction.class));
        counts.put(Milestone.class, getCountEstimate(Milestone.class));
        counts.put(StateDiff.class, getCountEstimate(StateDiff.class));
        counts.put(Hashes.class, getCountEstimate(Hashes.class));
    }

    @Override
    public void shutdown() {
        if (db != null) db.close();
        options.close();
        bloomFilter.close();
    }

    private final DoubleFunction<Object, Object> saveTransaction = (transaction, hash) ->
            db.put(transactionHandle, ((Hash) hash).bytes(), ((Transaction) transaction).bytes());

    private final DoubleFunction<Object, Object> saveMilestone = (milestone, index) ->
            db.put(milestoneHandle, Serializer.serialize((int)index), ((Milestone) milestone).hash.bytes());

    private final DoubleFunction<Object, Object> saveStateDiff = (stateDiff, hash) ->
            db.put(stateDiffHandle, ((Hash) hash).bytes(), ((StateDiff) stateDiff).bytes());

    private final DoubleFunction<Object, Object> saveHashes = (hashes, hash) ->
            db.put(hashesHandle, ((Hash)hash).bytes(), ((Hashes) hashes).bytes());

    @Override
    public boolean save(Persistable thing, Indexable index) throws Exception {
        ColumnFamilyHandle handle = classTreeMap.get().get(thing.getClass());
        if( !db.keyMayExist(handle, index.bytes(), new StringBuffer()) ) {
            counts.put(thing.getClass(), counts.get(thing.getClass()) + 1);
        }
        db.put(handle, index.bytes(), thing.bytes());
        return true;
    }

    @Override
    public void delete(Class<?> model, Indexable index) throws Exception {
        if( db.keyMayExist(classTreeMap.get().get(model), index.bytes(), new StringBuffer()) ) {
            counts.put(model, counts.get(model) + 1);
        }
        db.delete(classTreeMap.get().get(model), index.bytes());
    }

    @Override
    public boolean exists(Class<?> model, Indexable key) throws Exception {
        ColumnFamilyHandle handle = classTreeMap.get().get(model);
        return handle != null && db.get(handle, key.bytes()) != null;
    }

    @Override
    public Persistable latest(Class<?> model) throws Exception {
        final Persistable object;
        RocksIterator iterator = db.newIterator(classTreeMap.get().get(model));
        iterator.seekToLast();
        if(iterator.isValid()) {
            object = (Persistable) model.newInstance();
            object.read(iterator.value());
        } else {
            object = null;
        }
        iterator.close();
        return object;
    }

    @Override
    public Set<Indexable> keysWithMissingReferences(Class<?> model) throws Exception {
        ColumnFamilyHandle handle = classTreeMap.get().get(model);
        RocksIterator iterator = db.newIterator(handle);
        Set<Indexable> indexables = new HashSet<>();
        for(iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            if(db.get(hashesHandle, iterator.key()) == null) {
                indexables.add(new Hash(iterator.key()));
            }
        }
        iterator.close();
        return indexables;
    }

    @Override
    public Persistable get(Class<?> model, Indexable index) throws Exception {
        Persistable object = (Persistable) model.newInstance();
        object.read(db.get(classTreeMap.get().get(model), index == null? new byte[0]: index.bytes()));
        return object;
    }


    @Override
    public boolean mayExist(Class<?> model, Indexable index) throws Exception {
        return db.keyMayExist(classTreeMap.get().get(model), index.bytes(), new StringBuffer());
    }

    @Override
    public long count(Class<?> model) throws Exception {
        return counts.get(model);
    }

    private long getCountEstimate(Class<?> model) throws RocksDBException {
        ColumnFamilyHandle handle = classTreeMap.get().get(model);
        return db.getLongProperty(handle, "rocksdb.estimate-num-keys");
    }

    @Override
    public Set<Indexable> keysStartingWith(Class<?> modelClass, byte[] value) {
        RocksIterator iterator;
        ColumnFamilyHandle handle = classTreeMap.get().get(modelClass);
        Set<Indexable> keys = new HashSet<>();
        if(handle != null) {
            iterator = db.newIterator(handle);
            try {
                iterator.seek(new Hash(value, 0, value.length).bytes());
                for(;
                    iterator.isValid() && Arrays.equals(Arrays.copyOf(iterator.key(), value.length), value);
                    iterator.next()) {
                    keys.add(new Hash(iterator.key()));
                }
            } finally {
                iterator.close();
            }
        }
        return keys;
    }

    @Override
    public Persistable seek(Class<?> model, byte[] key) throws Exception {
        Set<Indexable> hashes = keysStartingWith(model, key);
        Indexable out;
        if(hashes.size() == 1) {
            out = (Indexable) hashes.toArray()[0];
        } else if (hashes.size() > 1) {
            out = (Indexable) hashes.toArray()[seed.nextInt(hashes.size())];
        } else {
            out = null;
        }
        return get(model, out);
    }

    @Override
    public Persistable next(Class<?> model, Indexable index) throws Exception {
        RocksIterator iterator = db.newIterator(classTreeMap.get().get(model));
        final Persistable object;
        iterator.seek(index.bytes());
        iterator.next();
        if(iterator.isValid()) {
            object = (Persistable) model.newInstance();
            object.read(iterator.value());
        } else {
            object = null;
        }
        iterator.close();
        return object;
    }

    @Override
    public Persistable  previous(Class<?> model, Indexable index) throws Exception {
        RocksIterator iterator = db.newIterator(classTreeMap.get().get(model));
        final Persistable object;
        iterator.seek(index.bytes());
        iterator.prev();
        if(iterator.isValid()) {
            object = (Persistable) model.newInstance();
            object.read(iterator.value());
        } else {
            object = null;
        }
        iterator.close();
        return object;
    }

    @Override
    public Persistable first(Class<?> model) throws Exception {
        RocksIterator iterator = db.newIterator(classTreeMap.get().get(model));
        final Persistable object;
        iterator.seekToFirst();
        if(iterator.isValid()) {
            object = (Persistable) model.newInstance();
            object.read(iterator.value());
        } else {
            object = null;
        }
        iterator.close();
        return object;
    }

    @Override
    public boolean merge(Persistable model, Indexable index) throws Exception {
        boolean exists = mayExist(model.getClass(), index);
        db.merge(classTreeMap.get().get(model.getClass()), index.bytes(), model.bytes());
        byte[] now = db.get(classTreeMap.get().get(model.getClass()), index.bytes());
        return exists;
    }

    private void flushHandle(ColumnFamilyHandle handle) throws RocksDBException {
        List<byte[]> itemsToDelete = new ArrayList<>();
        RocksIterator iterator = db.newIterator(handle);
        for(iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
            itemsToDelete.add(iterator.key());
        }
        iterator.close();
        if(itemsToDelete.size() > 0) {
            log.info("Flushing flags. Amount to delete: " + itemsToDelete.size());
        }
        for(byte[] itemToDelete: itemsToDelete) {
            db.delete(handle, itemToDelete);
        }
    }


    @Override
    public boolean update(Persistable thing, Indexable index, String item) throws Exception {
        return false;
    }

    public void createBackup(String path) throws RocksDBException {
        Env env;
        BackupableDBOptions backupableDBOptions;
        BackupEngine backupEngine;
        env = Env.getDefault();
        backupableDBOptions = new BackupableDBOptions(path);
        try {
            backupEngine = BackupEngine.open(env, backupableDBOptions);
            backupEngine.createNewBackup(db, true);
            backupEngine.close();
        } finally {
            env.close();
            backupableDBOptions.close();
        }
    }

    public void restoreBackup(String path, String logPath) throws Exception {
        Env env;
        BackupableDBOptions backupableDBOptions;
        BackupEngine backupEngine;
        env = Env.getDefault();
        backupableDBOptions = new BackupableDBOptions(path);
        backupEngine = BackupEngine.open(env, backupableDBOptions);
        shutdown();
        try(final RestoreOptions restoreOptions = new RestoreOptions(false)){
            backupEngine.restoreDbFromLatestBackup(path, logPath, restoreOptions);
        } finally {
            backupEngine.close();
        }
        backupableDBOptions.close();
        env.close();
        initDB(path, logPath);
    }

    private void initDB(String path, String logPath) throws Exception {
        MergeOperator myOperator = new StringAppendOperator();
        try {
            RocksDB.loadLibrary();
        } catch(Exception e) {
            if(SystemUtils.IS_OS_WINDOWS) {
                log.error("Error loading RocksDB library. " +
                        "Please ensure that " +
                        "Microsoft Visual C++ 2015 Redistributable Update 3 " +
                        "is installed and updated");
            }
            throw e;
        }
        Thread.yield();
        bloomFilter = new BloomFilter(BLOOM_FILTER_BITS_PER_KEY);
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig().setFilter(bloomFilter);
        options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setDbLogDir(logPath);

        List<ColumnFamilyHandle> familyHandles = new ArrayList<>();
        List<ColumnFamilyDescriptor> familyDescriptors = Arrays.stream(columnFamilyNames)
                .map(name -> new ColumnFamilyDescriptor(name.getBytes(),
                        new ColumnFamilyOptions()
                                .setMergeOperator(myOperator).setTableFormatConfig(blockBasedTableConfig))).collect(Collectors.toList());

        familyDescriptors.add(0, new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));

        db = RocksDB.open(options, path, familyDescriptors, familyHandles);
        db.enableFileDeletions(true);

        fillmodelColumnHandles(familyHandles);
    }

    private void fillMissingColumns(List<ColumnFamilyDescriptor> familyDescriptors, List<ColumnFamilyHandle> familyHandles, String path) throws Exception {
        List<ColumnFamilyDescriptor> columnFamilies = RocksDB.listColumnFamilies(new Options().setCreateIfMissing(true), path)
                .stream()
                .map(b -> new ColumnFamilyDescriptor(b, new ColumnFamilyOptions()))
                .collect(Collectors.toList());
        columnFamilies.add(0, familyDescriptors.get(0));
        List<ColumnFamilyDescriptor> missingFromDatabase = familyDescriptors.stream().filter(d -> columnFamilies.stream().filter(desc -> new String(desc.columnFamilyName()).equals(new String(d.columnFamilyName()))).toArray().length == 0).collect(Collectors.toList());
        List<ColumnFamilyDescriptor> missingFromDescription = columnFamilies.stream().filter(d -> familyDescriptors.stream().filter(desc -> new String(desc.columnFamilyName()).equals(new String(d.columnFamilyName()))).toArray().length == 0).collect(Collectors.toList());
        if (missingFromDatabase.size() != 0) {
            missingFromDatabase.remove(familyDescriptors.get(0));
            db = RocksDB.open(options, path, columnFamilies, familyHandles);
            for (ColumnFamilyDescriptor description : missingFromDatabase) {
                addColumnFamily(description.columnFamilyName(), db);
            }
            db.close();
        }
        if (missingFromDescription.size() != 0) {
            missingFromDescription.forEach(familyDescriptors::add);
        }
    }

    private void addColumnFamily(byte[] familyName, RocksDB db) throws RocksDBException {
        final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
                new ColumnFamilyDescriptor(familyName,
                        new ColumnFamilyOptions()));
        assert (columnFamilyHandle != null);
    }

    private void fillmodelColumnHandles(List<ColumnFamilyHandle> familyHandles) throws Exception {
        int i = 0;
        transactionHandle = familyHandles.get(++i);
        milestoneHandle = familyHandles.get(++i);
        stateDiffHandle = familyHandles.get(++i);
        hashesHandle = familyHandles.get(++i);

        for(; ++i < familyHandles.size();) {
            db.dropColumnFamily(familyHandles.get(i));
        }

        transactionGetList = new ArrayList<>();
        for(i = 1; i < 5; i ++) {
            transactionGetList.add(familyHandles.get(i));
        }
    }

    @FunctionalInterface
    private interface MyFunction<T, R> {
        R apply(T t) throws Exception;
    }

    @FunctionalInterface
    private interface IndexFunction<T> {
        void apply(T t) throws Exception;
    }

    @FunctionalInterface
    private interface DoubleFunction<T, I> {
        void apply(T t, I i) throws Exception;
    }

    @FunctionalInterface
    private interface MyRunnable<R> {
        R run() throws Exception;
    }
}
