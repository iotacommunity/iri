package com.iota.iri.storage;

import com.iota.iri.conf.Configuration;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.*;
import com.iota.iri.utils.Serializer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.SystemUtils;
import org.rocksdb.RocksIterator;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.SecureRandom;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by paul on 3/2/17 for iri.
 */
public class MemDBPersistenceProvider implements PersistenceProvider {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MemDBPersistenceProvider.class);

    private final Map<Hash, Transaction> transactionMap = new HashMap<>();
    private final Map<Hash, Address> addressMap = new HashMap<>();
    private final Map<Hash, Bundle> bundleMap = new HashMap<>();
    private final Map<Hash, Approvee> approveeMap = new HashMap<>();
    private final Map<Hash, Tag> tagMap = new HashMap<>();
    private final Map<Hash, Tip> tipMap = new HashMap<>();
    private final TreeMap<Integer, Milestone> milestoneMap = new TreeMap<>();

    private final Map<Class<?>, Map> classTreeMap = new HashMap<>();
    private final Map<Class<?>, MyFunction<Object, Boolean>> saveMap = new HashMap<>();
    private final Map<Class<?>, MyFunction<Object, Void>> deleteMap = new HashMap<>();
    private final Map<Class<?>, MyFunction<Object, Object>> setKeyMap = new HashMap<>();
    private final Map<Class<?>, MyFunction<Object, Boolean>> loadMap = new HashMap<>();
    private final Map<Class<?>, MyFunction<Object, Boolean>> mayExistMap = new HashMap<>();
    private final Map<Class<?>, MyFunction<Object, Object>> nextMap = new HashMap<>();
    private final Map<Class<?>, MyFunction<Object, Object>> prevMap = new HashMap<>();
    private final Map<Class<?>, MyRunnable<Object>> latestMap = new HashMap<>();
    private final Map<Class<?>, MyRunnable<Object>> firstMap = new HashMap<>();
    private final Map<Class<?>, Map> countMap = new HashMap<>();

    private final SecureRandom seed = new SecureRandom();

    private boolean available;

    @Override
    public void init() throws Exception {
        restoreBackup(Configuration.string(Configuration.DefaultConfSettings.DB_PATH));
        initClassTreeMap();
        initSaveMap();
        initLoadMap();
        initSetKeyMap();
        initMayExistMap();
        initDeleteMap();
        initCountMap();
        initIteratingMaps();
        available = true;
    }

    @Override
    public boolean isAvailable() {
        return this.available;
    }

    private void initIteratingMaps() {
        firstMap.put(Milestone.class, firstMilestone);
        latestMap.put(Milestone.class, latestMilestone);
        nextMap.put(Milestone.class, nextMilestone);
        prevMap.put(Milestone.class, previousMilestone);
    }

    private void initSetKeyMap() {
        setKeyMap.put(Transaction.class, hashObject -> new Transaction(((Hash) hashObject)));
    }

    private void initCountMap() {
        countMap.put(Transaction.class, transactionMap);
        countMap.put(Address.class, addressMap);
        countMap.put(Bundle.class, bundleMap);
        countMap.put(Approvee.class, approveeMap);
        countMap.put(Tag.class, tagMap);
        countMap.put(Tip.class, tipMap);
        /*
        countMap.put(AnalyzedFlag.class, analyzedFlagHandle);
        */
    }

    private void initDeleteMap() {
        deleteMap.put(Transaction.class, txObj -> {
            Transaction transaction = ((Transaction) txObj);
            transactionMap.remove(transaction.hash);
            return null;
        });
        deleteMap.put(Tip.class, txObj -> {
            tipMap.remove(((Tip) txObj).hash);
            return null;
        });
        deleteMap.put(Milestone.class, msObj -> {
            milestoneMap.remove(((Milestone) msObj).index);
            return null;
        });
    }

    private void initMayExistMap() {
        mayExistMap.put(Transaction.class, txObj ->
                transactionMap.containsKey(((Transaction) txObj).hash)
        );
        mayExistMap.put(Tip.class, txObj ->
                tipMap.containsKey(((Tip) txObj).hash)
        );
    }

    private void initLoadMap() {
        loadMap.put(Transaction.class, getTransaction);
        loadMap.put(Address.class, getAddress);
        loadMap.put(Tag.class, getTag);
        loadMap.put(Bundle.class, getBundle);
        loadMap.put(Approvee.class, getApprovee);
        loadMap.put(Milestone.class, getMilestone);
    }

    private void initSaveMap() {
        saveMap.put(Transaction.class, saveTransaction);
        saveMap.put(Tip.class, saveTip);
        saveMap.put(Milestone.class, saveMilestone);
    }

    private void initClassTreeMap() {
        classTreeMap.put(Address.class, addressMap);
        classTreeMap.put(Tip.class, tipMap);
        classTreeMap.put(Approvee.class, approveeMap);
        classTreeMap.put(Bundle.class, bundleMap);
        classTreeMap.put(Tag.class, tagMap);
        classTreeMap.put(Transaction.class, transactionMap);
    }

    @Override
    public void shutdown() {
        try {
            createBackup(Configuration.string(Configuration.DefaultConfSettings.DB_PATH));
        } catch (IOException e) {
            log.error("Could not create memdb backup.");
        }
        transactionMap.clear();
        addressMap.clear();
        bundleMap.clear();
        approveeMap.clear();
        tagMap.clear();
        tipMap.clear();
        milestoneMap.clear();
    }

    private final MyFunction<Object, Boolean> saveTransaction = (txObject -> {
        Transaction transaction = (Transaction) txObject;
        byte[] key = transaction.hash.bytes();
        transactionMap.put(transaction.hash, transaction);
        Address address = addressMap.get(transaction.address.hash);
        transaction.address.transactions = new Hash[]{transaction.hash};
        if(address != null) {
            transaction.address.transactions = ArrayUtils.addAll(address.transactions, transaction.address.transactions);
        }
        addressMap.put(transaction.address.hash, transaction.address);
        Bundle bundle = bundleMap.get(transaction.bundle.hash);
        transaction.bundle.transactions = new Hash[]{transaction.hash};
        if(bundle != null) {
            transaction.bundle.transactions = ArrayUtils.addAll(bundle.transactions, transaction.bundle.transactions);
        }
        bundleMap.put(transaction.bundle.hash, transaction.bundle);
        Approvee trunk = approveeMap.get(transaction.trunk.hash);
        transaction.trunk.transactions = new Hash[]{transaction.hash};
        if(trunk != null) {
            transaction.trunk.transactions = ArrayUtils.addAll(trunk.transactions, transaction.trunk.transactions);
        }
        approveeMap.put(transaction.trunk.hash, transaction.trunk);
        transaction.branch.transactions = new Hash[]{transaction.hash};
        Approvee branch = approveeMap.get(transaction.trunk.hash);
        if(trunk != null) {
            transaction.branch.transactions = ArrayUtils.addAll(branch.transactions, transaction.branch.transactions);
        }
        approveeMap.put(transaction.branch.hash, transaction.branch);
        Tag tag = tagMap.get(transaction.tag.value);
        transaction.tag.transactions = new Hash[]{transaction.hash};
        if(tag != null) {
            transaction.tag.transactions = ArrayUtils.addAll(tag.transactions, transaction.tag.transactions);
        }
        tagMap.put(transaction.tag.value, transaction.tag);
        return true;
    });

    private final MyFunction<Object, Boolean> saveTip = tipObj -> {
        tipMap.put(((Tip) tipObj).hash, (Tip) tipObj);
        return true;
    };

    private final MyFunction<Object, Boolean> saveMilestone = msObj -> {
        Milestone milestone = ((Milestone) msObj);
        milestoneMap.put(milestone.index, milestone);
        return true;
    };

    private byte[] objectBytes(Object o) throws IOException {
        byte[] output;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(o);
        oos.close();
        output = bos.toByteArray();
        bos.close();
        return output;
    }

    private Object objectFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        Object out = null;
        if(bytes.length > 0) {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            out = ois.readObject();
            ois.close();
            bis.close();
        }
        return out;
    }

    @Override
    public boolean save(Object thing) throws Exception {
        return saveMap.get(thing.getClass()).apply(thing);
    }

    @Override
    public void delete(Object thing) throws Exception {
        deleteMap.get(thing.getClass()).apply(thing);
    }

    private Hash[] byteToHash(byte[] bytes, int size) {
        if(bytes == null) {
            return new Hash[0];
        }
        int i;
        Set<Hash> hashes = new HashSet<>();
        for(i = size; i <= bytes.length; i += size + 1) {
            hashes.add(new Hash(Arrays.copyOfRange(bytes, i - size, i)));
        }
        return hashes.stream().toArray(Hash[]::new);
    }

    @Override
    public boolean exists(Class<?> model, Hash key) throws Exception {
        Map map = classTreeMap.get(model);
        return map != null && map.containsKey(key);
    }

    @Override
    public Object latest(Class<?> model) throws Exception {
        MyRunnable<Object> locator = latestMap.get(model);
        if(locator != null) {
            return locator.run();
        }
        return null;
    }

    @Override
    public Object[] keysWithMissingReferences(Class<?> modelClass) throws Exception {
        if(modelClass == Transaction.class) {
            return getMissingTransactions();
        }
        if(modelClass == Tip.class) {
            List<byte[]> tips = new ArrayList<>();
            return transactionMap.keySet().parallelStream().filter(h -> !approveeMap.containsKey(h)).toArray();
        }
        throw new NotImplementedException("Not implemented");
    }

    private Object[] getMissingTransactions() {
        return approveeMap.keySet().parallelStream().filter(h -> !transactionMap.containsKey(h)).toArray();
    }

    @Override
    public boolean get(Object model) throws Exception {
        return loadMap.get(model.getClass()).apply(model);
    }

    private final MyFunction<Object, Boolean> getTransaction = (txObject) -> {
        if(txObject == null) {
            return false;
        }
        Transaction transaction = ((Transaction) txObject);
        Transaction mapTx = transactionMap.get(transaction.hash);
        if(mapTx == null) {
            transaction.type = TransactionViewModel.PREFILLED_SLOT;
            return false;
        }
        transaction.bytes = mapTx.bytes;
        transaction.validity = mapTx.validity;
        transaction.type = mapTx.type;
        transaction.arrivalTime = mapTx.arrivalTime;
        transaction.solid = mapTx.solid;
        transaction.snapshot = mapTx.snapshot;
        transaction.height = mapTx.height;
        transaction.sender = mapTx.sender;
        return true;
    };

    private boolean byteToBoolean(byte[] bytes) {
        return !(bytes == null || bytes.length != 1) && bytes[0] != 0;
    }

    private final MyFunction<Object, Boolean> getAddress = (addrObject) -> {
        Address address = ((Address) addrObject);
        Address result = addressMap.get(address.hash);
        if(result == null) {
            address.transactions = new Hash[0];
            return false;
        } else {
            address.transactions = result.transactions;
            return  true;
        }
    };

    private final MyFunction<Object, Boolean> getTag = tagObj -> {
        Tag tag = ((Tag) tagObj);
        Tag result = tagMap.get(tag.value);
        if(result == null) {
            tag.transactions = new Hash[0];
            return false;
        } else {
            tag.transactions = result.transactions;
            return  true;
        }
    };

    private final MyFunction<Object, Boolean> getMilestone = msObj -> {
        if(msObj instanceof Milestone) {
            Milestone milestone = ((Milestone) msObj);
            Milestone result = milestoneMap.get(milestone.index);
            if(result != null) {
                milestone.hash = result.hash;
                milestone.snapshot = result.snapshot;
                return true;
            }
        }
        return false;
    };

    private MyRunnable<Object> firstMilestone = () -> milestoneMap.isEmpty()? null: milestoneMap.firstEntry().getValue();

    private MyRunnable<Object> latestMilestone = () -> milestoneMap.isEmpty()? null: milestoneMap.lastEntry().getValue();

    private MyFunction<Object, Object> nextMilestone = (start) -> milestoneMap.isEmpty()? null: milestoneMap.ceilingEntry((int)start + 1).getValue();

    private MyFunction<Object, Object> previousMilestone = (start) -> milestoneMap.isEmpty()? null: milestoneMap.floorEntry((int)start - 1).getValue();

    private final MyFunction<Object, Boolean> getBundle = bundleObj -> {
        Bundle bundle = ((Bundle) bundleObj);
        Bundle result = bundleMap.get(bundle.hash);
        if(result != null) {
            bundle.transactions = result.transactions;
            return true;
        }
        return false;
    };

    private final MyFunction<Object, Boolean> getApprovee = approveeObj -> {
        Approvee approvee = ((Approvee) approveeObj);
        Approvee result = approveeMap.get(approvee.hash);
        if(result == null) {
            approvee.transactions = new Hash[0];
            return false;
        } else {
            approvee.transactions = result.transactions;
            return true;
        }
    };

    @Override
    public boolean mayExist(Object model) throws Exception {
        return mayExistMap.get(model.getClass()).apply(model);
    }

    @Override
    public long count(Class<?> model) throws Exception {
        Map map = classTreeMap.get(model);
        return map == null ? 0 : map.size();
    }

    @Override
    public Hash[] keysStartingWith(Class<?> modelClass, byte[] value) {
        Map handle = classTreeMap.get(modelClass);
        if(handle != null) {
            Set<Hash> keySet = handle.keySet();
            return keySet.parallelStream().filter(h -> Arrays.equals(Arrays.copyOf(h.bytes(), value.length), value)).toArray(Hash[]::new);
        }
        return new Hash[0];
    }

    @Override
    public Object seek(Class<?> model, byte[] key) throws Exception {
        Hash[] hashes = keysStartingWith(model, key);
        Object out;
        if(hashes.length == 1) {
            out = setKeyMap.get(model).apply(hashes[0]);
        } else if (hashes.length > 1) {
            out = setKeyMap.get(model).apply(hashes[seed.nextInt(hashes.length)]);
        } else {
            out = null;
        }
        loadMap.get(model).apply(out);
        return out;
    }

    @Override
    public Object next(Class<?> model, int index) throws Exception {
        MyFunction<Object, Object> locator = nextMap.get(model);
        if(locator != null) {
            return locator.apply(index);
        }
        return null;
    }

    @Override
    public Object previous(Class<?> model, int index) throws Exception {
        MyFunction<Object, Object> locator = prevMap.get(model);
        if(locator != null) {
            return locator.apply(index);
        }
        return null;
    }

    @Override
    public Object first(Class<?> model) throws Exception {
        MyRunnable<Object> locator = firstMap.get(model);
        if(locator != null) {
            return locator.run();
        }
        return null;
    }

    private MyFunction<Object, Boolean> updateTransaction() {
        return txObject -> {
            Transaction transaction = (Transaction) txObject;
            transactionMap.put(transaction.hash, transaction);
            return true;
        };
    }

    private MyFunction<Object, Boolean> updateMilestone() {
        return msObj -> {
            Milestone milestone = ((Milestone) msObj);
            milestoneMap.put(milestone.index, milestone);
            return true;
        };
    }

    @Override
    public boolean update(Object thing, String item) throws Exception {
        if(thing instanceof Transaction) {
            return updateTransaction().apply(thing);
        } else if (thing instanceof Milestone){
            return updateMilestone().apply(thing);
        }
        throw new NotImplementedException("Update for object " + thing.getClass().getName() + " is not implemented yet.");
    }

    public void createBackup(String path) throws IOException {
        saveBytes(path + "transaction.map",objectBytes(transactionMap));
        saveBytes(path + "bundle.map",objectBytes(bundleMap));
        saveBytes(path + "approvee.map",objectBytes(approveeMap));
        saveBytes(path + "tag.map",objectBytes(tagMap));
        saveBytes(path + "tip.map",objectBytes(tipMap));
        saveBytes(path + "milestone.map",objectBytes(milestoneMap));
    }

    private void saveBytes(String path, byte[] bytes) throws IOException {
        File file = new File(path);
        file.createNewFile();
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(bytes, 0, bytes.length);
        fos.flush();
        fos.close();
    }

    public void restoreBackup(String path) throws Exception {
        Object db = objectFromBytes(loadBytes(path + "/transaction.map"));
        if(db != null) {
            transactionMap.putAll((Map<Hash, Transaction>) db);
        }
        db = objectFromBytes(loadBytes(path + "/bundle.map"));
        if(db != null) {
            bundleMap.putAll((Map<Hash, Bundle>) db);
        }
        db = objectFromBytes(loadBytes(path + "/approvee.map"));
        if(db != null) {
            approveeMap.putAll((Map<Hash, Approvee>) db);
        }
        db = objectFromBytes(loadBytes(path + "/tag.map"));
        if(db != null) {
            tagMap.putAll((Map<Hash, Tag>) db);
        }
        db = objectFromBytes(loadBytes(path + "/tip.map"));
        if(db != null) {
            tipMap.putAll((Map<Hash, Tip>) db);
        }
        db = objectFromBytes(loadBytes(path + "/milestone.map"));
        if(db != null) {
            milestoneMap.putAll((TreeMap<Integer, Milestone>) db);
        }
    }

    private byte[] loadBytes(String path) throws IOException {
        File inputFile = new File(path);
        if(inputFile.exists()) {
            byte[] data = new byte[(int) inputFile.length()];
            FileInputStream fis = new FileInputStream(inputFile);
            fis.read(data, 0, data.length);
            fis.close();
            return data;
        }
        return new byte[0];
    }

    @FunctionalInterface
    private interface MyFunction<T, R> {
        R apply(T t) throws Exception;
    }

    @FunctionalInterface
    private interface MyRunnable<R> {
        R run() throws Exception;
    }
}
