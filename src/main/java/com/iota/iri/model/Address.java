package com.iota.iri.model;

import java.io.Serializable;

/**
 * Created by paul on 3/6/17 for iri.
 */
public class Address implements Serializable {
    public Hash hash;
    public Hash[] transactions;
}
