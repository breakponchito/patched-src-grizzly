/*
 * Copyright (c) 2012, 2020 Oracle and/or its affiliates. All rights reserved.
 * Copyright (c) 2018 Payara Services Ltd.
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
module org.glassfish.grizzly.http {
    
    exports org.glassfish.grizzly.http;
    exports org.glassfish.grizzly.http.io;
    exports org.glassfish.grizzly.http.util;
    
    opens org.glassfish.grizzly.http;
    opens org.glassfish.grizzly.http.io;
    opens org.glassfish.grizzly.http.util;

    requires java.logging;
    requires org.glassfish.grizzly;
}
