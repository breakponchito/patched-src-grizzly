/*
 * Copyright (c) 2013, 2020 Oracle and/or its affiliates. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0, which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the
 * Eclipse Public License v. 2.0 are satisfied: GNU General Public License,
 * version 2 with the GNU Classpath Exception, which is available at
 * https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 */

package org.glassfish.grizzly;

/**
 * This class will be passed to {@link GracefulShutdownListener} instances registered against a {@link Transport}.
 *
 * @since 2.3.5
 */
public interface ShutdownContext {

    /**
     * @return the Transport that is being shutdown.
     */
    Transport getTransport();

    /**
     * Invoked by called {@link GracefulShutdownListener} to notify the graceful termination process that it's safe to
     * terminate the transport.
     */
    void ready();

}
