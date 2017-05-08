package com.iota.iri.controllers;

import com.iota.iri.model.Hashes;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created by paul on 5/6/17.
 */
public class HashesViewModel {
    private Hashes self;
    private Hash hash;

    public HashesViewModel(Hash hash) {
        this.hash = hash;
    }

    private HashesViewModel(Hashes hashes, Hash hash) {
        self = hashes == null || hashes.set == null ? new Hashes(): hashes;
        this.hash = hash;
    }

    public static HashesViewModel load(Hash hash) throws Exception {
        return new HashesViewModel((Hashes) Tangle.instance().load(Hashes.class, hash), hash);
    }

    public static boolean merge(Hash hash, Hash hashToMerge) throws Exception {
        Hashes hashes = new Hashes();
        hashes.set = new HashSet<>(Collections.singleton(hashToMerge));
        return Tangle.instance().merge(hashes, hash);
    }

    public boolean store() throws Exception {
        return Tangle.instance().save(self, hash);
    }

    public boolean addHash(Hash theHash) {
        return getHashes().add(theHash);
    }

    public Hash getHash() {
        return hash;
    }

    public Set<Hash> getHashes() {
        if(self.set == null) {
            self.set = new HashSet<>();
        }
        return self.set;
    }
}
