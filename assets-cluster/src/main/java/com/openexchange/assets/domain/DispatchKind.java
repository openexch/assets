// SPDX-License-Identifier: Apache-2.0
package com.openexchange.assets.domain;

/** ME command kind carried by MeDispatch (0..2 equal the ME commandKind). Codes equal the wire values. */
public enum DispatchKind {
    CREATE(0),
    CANCEL(1),
    AMEND(2),
    RELEASE_ID(3);

    private final int code;

    DispatchKind(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
