// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.determinism;

/**
 * The traffic generator's own view of one core order and of its current ME leg. The generator never reads the
 * engine: every field here is predicted from the lines the generator itself wrote, with the engine's rules
 * (S4 admission, S5 attempts and terminals, S6 triggers and slices) mirrored by hand. A wrong prediction shows up
 * as a stale outcome in the engine, which {@code ProductionShapeEquivalenceTest} asserts is zero.
 */
final class LifecycleModelOrder {

    static final int LIMIT = 0;
    static final int MARKET = 1;
    static final int LIMIT_MAKER = 2;
    static final int STOP_LOSS = 3;
    static final int STOP_LIMIT = 4;
    static final int TRAILING = 5;
    static final int ICEBERG = 6;

    final long orderId;
    final long user;
    final int market;
    final int side;
    final int type;

    // core record (predicted)
    long price;
    long quantity;
    long filled;
    long stop;
    long delta;
    long display;
    long hidden;
    long sliceRemaining;
    long hold;
    long cid;
    long exp;
    long trailArm;
    int attempt;
    boolean armed;
    boolean terminal;

    // current ME leg (the fake ME's resting order)
    long leg;
    long legRemaining;
    long legPrice;
    long legBudget;
    long arrival;
    int meType;
    boolean resting;

    LifecycleModelOrder(final long orderId, final long user, final int market, final int side, final int type) {
        this.orderId = orderId;
        this.user = user;
        this.market = market;
        this.side = side;
        this.type = type;
    }

    boolean buy() {
        return side == 0;
    }

    long remaining() {
        return quantity - filled;
    }

    boolean isStop() {
        return type == STOP_LOSS || type == STOP_LIMIT;
    }
}
