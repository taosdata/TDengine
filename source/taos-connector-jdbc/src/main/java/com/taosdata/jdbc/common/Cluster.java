package com.taosdata.jdbc.common;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Cluster {
    // Immutable set: nodes cannot be modified after construction
    private final Set<Endpoint> endpoints;

    /**
     * Constructs a Cluster with variable endpoints.
     * @param endpoints the nodes that form the cluster
     */
    public Cluster(Endpoint... endpoints) {
        // Convert to immutable set to prevent external modification
        Set<Endpoint> temp = new HashSet<>();
        Collections.addAll(temp, endpoints);
        this.endpoints = Collections.unmodifiableSet(temp);
    }

    /**
     * Constructs a Cluster with a set of endpoints.
     * @param endpoints the nodes that form the cluster
     */
    public Cluster(Set<Endpoint> endpoints) {
        this.endpoints = Collections.unmodifiableSet(new HashSet<>(endpoints));
    }

    /**
     * Gets all nodes in the cluster (unmodifiable).
     * @return an immutable set of endpoints
     */
    public Set<Endpoint> getEndpoints() {
        return endpoints;
    }

    /**
     * Checks equality: two clusters are equal if they contain exactly the same endpoints (order-agnostic).
     * @param o the object to compare
     * @return true if endpoint sets are identical
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cluster cluster = (Cluster) o;
        return Objects.equals(endpoints, cluster.endpoints);
    }

    /**
     * Generates hash code based on the endpoint set.
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(endpoints);
    }

    @Override
    public String toString() {
        return "Cluster{endpoints=" + endpoints + "}";
    }
}