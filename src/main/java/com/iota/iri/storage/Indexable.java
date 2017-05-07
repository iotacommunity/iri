package com.iota.iri.storage;

/**
 * Created by paul on 5/6/17.
 */
public interface Indexable extends Comparable<Indexable> {
    byte[] bytes();
    Indexable incremented();
    Indexable decremented();
}
