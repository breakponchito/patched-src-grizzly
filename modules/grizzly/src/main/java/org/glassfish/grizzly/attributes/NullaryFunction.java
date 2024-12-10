/*
 * Copyright (c) 2008, 2020 Oracle and/or its affiliates. All rights reserved.
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

package org.glassfish.grizzly.attributes;

/**
 * {@link Attribute} initializer.
 *
 * Is used by {@link Attribute#get(AttributeHolder)}, if there is no attribute value stored in {@link AttributeHolder},
 * or attribute value is <tt>null</tt>.
 *
 * @see Attribute
 * @see AttributeHolder
 *
 * @author Ken Cavanaugh
 * @deprecated pls. use {@link org.glassfish.grizzly.utils.NullaryFunction}
 */
@Deprecated
public interface NullaryFunction<T> extends org.glassfish.grizzly.utils.NullaryFunction<T> {
}
