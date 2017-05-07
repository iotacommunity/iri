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

    private static List<Hash> tips = new ArrayList<>();
    private static List<Hash> solidTips = new ArrayList<>();
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

    public static Hash[] getTips() {
        Hash[] hashes;
        synchronized (sync) {
            hashes = ArrayUtils.addAll(tips.stream().toArray(Hash[]::new), solidTips.stream().toArray(Hash[]::new));
        }
        return hashes;
    }
    public static Hash getRandomSolidTipHash() {
        synchronized (sync) {
            return solidTips.size() != 0 ? solidTips.get(seed.nextInt(solidTips.size())) : getRandomNonSolidTipHash();
        }
    }

    public static Hash getRandomNonSolidTipHash() {
        synchronized (sync) {
            return tips.size() != 0 ? tips.get(seed.nextInt(tips.size())) : null;
        }
    }

    public static Hash getRandomTipHash() throws ExecutionException, InterruptedException {
        synchronized (sync) {
            if(size() == 0) {
                return null;
            }
            int index = seed.nextInt(size());
            if(index >= tips.size()) {
                index -= tips.size();
                return solidTips.get(index);
            }
            return tips.get(index);
        }
    }

    public static int nonSolidSize() {
        return tips.size();
    }

    public static int size() {
        return tips.size() + solidTips.size();
    }

    public static void loadTipHashes() throws ExecutionException, InterruptedException {
        Set<Indexable> hashes = Tangle.instance()
                .keysWithMissingReferences(Transaction.class).get();
        if(hashes != null) {
            tips.addAll(hashes.stream().map(h -> (Hash) h).collect(Collectors.toList()));
        }
    }
}
