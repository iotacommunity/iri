package com.iota.iri.controllers;

import com.iota.iri.model.Hash;
import com.iota.iri.model.Transaction;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Tangle;
import org.apache.commons.lang3.ArrayUtils;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Created by paul on 3/14/17 for iri-testnet.
 */
public class TipsViewModel {

    private static Set<Hash> tips = new HashSet<>();
    private static Set<Hash> solidTips = new HashSet<>();
    private static SecureRandom seed = new SecureRandom();
    public static final Object sync = new Object();

    public static boolean addTipHash (Hash hash) throws ExecutionException, InterruptedException {
        synchronized (sync) {
            return tips.add(hash);
        }
    }

    public static boolean removeTipHash (Hash hash) throws ExecutionException, InterruptedException {
        synchronized (sync) {
            if(!tips.remove(hash)) {
                return solidTips.remove(hash);
            }
        }
        return true;
    }

    public static void setSolid(Hash tip) {
        synchronized (sync) {
            if(!tips.remove(tip)) {
                solidTips.add(tip);
            }
        }
    }

    public static Set<Hash> getTips() {
        Set<Hash> hashes = new HashSet<>();
        synchronized (sync) {
            hashes.addAll(tips);
            hashes.addAll(solidTips);
        }
        return hashes;
    }
    public static Hash getRandomSolidTipHash() {
        synchronized (sync) {
            int index = seed.nextInt(solidTips.size());
            Iterator<Hash> hashIterator;
            hashIterator = solidTips.iterator();
            while(--index > 0 && hashIterator.hasNext()){ hashIterator.next();}
            return hashIterator.next();
            //return solidTips.size() != 0 ? solidTips.get(seed.nextInt(solidTips.size())) : getRandomNonSolidTipHash();
        }
    }

    public static Hash getRandomNonSolidTipHash() {
        synchronized (sync) {
            int index = seed.nextInt(tips.size());
            Iterator<Hash> hashIterator;
            hashIterator = tips.iterator();
            while(--index > 0 && hashIterator.hasNext()){ hashIterator.next();}
            return hashIterator.next();
            //return tips.size() != 0 ? tips.get(seed.nextInt(tips.size())) : null;
        }
    }

    public static Hash getRandomTipHash() throws ExecutionException, InterruptedException {
        synchronized (sync) {
            if(size() == 0) {
                return null;
            }
            int index = seed.nextInt(size());
            if(index >= tips.size()) {
                return getRandomSolidTipHash();
            } else {
                return getRandomNonSolidTipHash();
            }
        }
    }

    public static int nonSolidSize() {
        return tips.size();
    }

    public static int size() {
        return tips.size() + solidTips.size();
    }

    public static void loadTipHashes() throws Exception {
        Set<Indexable> hashes = Tangle.instance()
                .keysWithMissingReferences(Transaction.class);
        if(hashes != null) {
            tips.addAll(hashes.stream().map(h -> (Hash) h).collect(Collectors.toList()));
        }
    }
}
