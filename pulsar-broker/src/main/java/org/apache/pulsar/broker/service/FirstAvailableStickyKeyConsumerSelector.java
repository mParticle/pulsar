/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.client.api.Range;

/**
 * This implementation tracks outstanding messages for each consumer to ensure messages for the same/similar key
 * are never sent to multiple consumers at the same time, but otherwise provides no specific consumer routing.
 */
public class FirstAvailableStickyKeyConsumerSelector implements StickyKeyConsumerSelector {

    private final Map<Consumer, Map<Integer, Integer>> consumerAssignments;
    private final Map<Integer, Consumer> hashAssignments;
    private final Map<Position, Integer> entryHashes;

    public FirstAvailableStickyKeyConsumerSelector() {
        consumerAssignments = new HashMap<Consumer, Map<Integer, Integer>>();
        hashAssignments = new HashMap<Integer, Consumer>();
        entryHashes = new HashMap<Position, Integer>();
    }

    @Override
    public void addConsumer(Consumer consumer) throws ConsumerAssignException {
        synchronized (this) {
            // Insert the new consumer in the assignments tracking map
            consumerAssignments.put(consumer, new HashMap<Integer, Integer>());
        }
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        synchronized (this) {
            // We may want to clean up the entryHashes map after losing a consumer but for now
            // it seems safe to assume those messages will eventually get redelivered/reinserted anyhow.
            // Removing the consumer from the assignments tracker will suffice to stop it getting new work.
            Map<Integer, Integer> removedConsumerHashes = consumerAssignments.remove(consumer);
            for (Integer hashToRemove: removedConsumerHashes.keySet()) {
                hashAssignments.remove(hashToRemove);
            }
        }
    }

    @Override
    public Consumer select(int hash, Position entryPosition) {
        synchronized (this) {
            if (consumerAssignments.isEmpty()) {
                // There are no consumers
                return null;
            }

            // Insert our entry into the entry hash tracker.  We'll need this to decrement the consumerAssignemnts
            // count once the message is acknowledged.
            entryHashes.put(entryPosition, hash);

            // This may be an ugly way to do this?
            return hashAssignments.compute(hash, (key, consumer) -> {
                if (consumer == null) {
                    // No consumer is currently working this routing key, find the next available.
                    // For now just pick the first one we find with permits
                    for (Consumer potential: consumerAssignments.keySet()) {
                        if (potential.getAvailablePermits() > 0) {
                            consumer = potential;
                            break;
                        }
                    }
                }

                // We will use this body to increment the assignment reference count as a side-effect.
                if (consumer != null) {
                    consumerAssignments.compute(consumer, (unused, referenceCounts) -> {
                        referenceCounts.compute(key, (unused2, currentCount) -> currentCount + 1);
                        return referenceCounts;
                    });
                }

                return consumer;
            });
        }
    }

    @Override
    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        Map<Consumer, List<Range>> result = new LinkedHashMap<>();
        synchronized (this) {
            // This will return the currently assigned hashes for each consumer.
            // Not sure if that's useful or not; might not be worth returning anything at all here.
            for (Map.Entry<Consumer, Map<Integer, Integer>> entry: consumerAssignments.entrySet()) {
                for (Integer hash: entry.getValue().keySet()) {
                    result.computeIfAbsent(entry.getKey(), key -> new ArrayList<>())
                            .add(Range.of(hash, hash));
                }
            }
        }
        return result;
    }
}
