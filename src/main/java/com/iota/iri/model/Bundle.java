package com.iota.iri.model;

import java.io.Serializable;

/**
 * Created by paul on 3/8/17 for iri.
 */
public class Bundle implements Serializable{
    public Hash hash;
    public Hash[] transactions;
}
