package com.iota.iri.storage;

/**
 * Created by paul on 5/6/17.
 */
public interface Persistable {
    byte[] bytes();
    void read(byte[] bytes);
}
