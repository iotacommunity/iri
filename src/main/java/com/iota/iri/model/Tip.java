package com.iota.iri.model;

import java.io.Serializable;

/**
 * Created by paul on 3/8/17 for iri.
 */
public class Tip implements Serializable {
    public final Hash hash;
    public final byte[] status = new byte[]{0};

    public Tip(Hash hashBytes) {
        this.hash = hashBytes;
    }
}
