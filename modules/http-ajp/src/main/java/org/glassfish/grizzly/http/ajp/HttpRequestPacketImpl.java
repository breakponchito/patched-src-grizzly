/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2011 Oracle and/or its affiliates. All rights reserved.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common Development
 * and Distribution License("CDDL") (collectively, the "License").  You
 * may not use this file except in compliance with the License.  You can
 * obtain a copy of the License at
 * https://glassfish.dev.java.net/public/CDDL+GPL_1_1.html
 * or packager/legal/LICENSE.txt.  See the License for the specific
 * language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice in each
 * file and include the License file at packager/legal/LICENSE.txt.
 *
 * GPL Classpath Exception:
 * Oracle designates this particular file as subject to the "Classpath"
 * exception as provided by Oracle in the GPL Version 2 section of the License
 * file that accompanied this code.
 *
 * Modifications:
 * If applicable, add the following below the License Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyright [year] [name of copyright owner]"
 *
 * Contributor(s):
 * If you wish your version of this file to be governed by only the CDDL or
 * only the GPL Version 2, indicate your decision by adding "[Contributor]
 * elects to include this software in this distribution under the [CDDL or GPL
 * Version 2] license."  If you don't indicate a single choice of license, a
 * recipient has the option to distribute your version of this file under
 * either the CDDL, the GPL Version 2 or to extend the choice of license to
 * its licensees as provided above.  However, if you add GPL Version 2 code
 * and therefore, elected the GPL Version 2 license, then the option applies
 * only if the new code is made subject to such option by the copyright
 * holder.
 */

package org.glassfish.grizzly.http.ajp;

import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.ThreadCache;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.http.ProcessingState;
import org.glassfish.grizzly.http.util.BufferChunk;
import org.glassfish.grizzly.http.util.DataChunk;
import org.glassfish.grizzly.ssl.SSLSupport;
import org.glassfish.grizzly.utils.BufferInputStream;

/**
 *
 * @author oleksiys
 */
final class HttpRequestPacketImpl extends HttpRequestPacket {
    private static final Logger LOGGER = Grizzly.logger(HttpRequestPacketImpl.class);
    
    private static final ThreadCache.CachedTypeIndex<HttpRequestPacketImpl> CACHE_IDX =
            ThreadCache.obtainIndex(HttpRequestPacketImpl.class, 2);

    public static HttpRequestPacketImpl create() {
        HttpRequestPacketImpl httpRequestImpl =
                ThreadCache.takeFromCache(CACHE_IDX);
        if (httpRequestImpl == null) {
            httpRequestImpl = new HttpRequestPacketImpl();
        }

        return httpRequestImpl.init();
    }

    private final HttpResponsePacketImpl cachedResponse = new HttpResponsePacketImpl();
    
    final ProcessingState processingState = new ProcessingState();

    private int contentBytesRemaining = -1;

    private HttpRequestPacketImpl() {
    }

    @Override
    public Object getAttribute(final String name) {
        Object result = super.getAttribute(name);
        
        // If it's CERTIFICATE_KEY request - lazy initialize it, if required
        if (result == null && SSLSupport.CERTIFICATE_KEY.equals(name)) {
            final DataChunk certString = getNote(AjpHandlerFilter.SSL_CERT_NOTE);
            // Extract SSL certificate information (if requested)
            if (certString != null && !certString.isNull()) {
                final BufferChunk bc = certString.getBufferChunk();
                BufferInputStream bais = new BufferInputStream(bc.getBuffer(),
                        bc.getStart(), bc.getEnd());

                // Fill the first element.
                X509Certificate jsseCerts[] = null;
                try {
                    CertificateFactory cf =
                            CertificateFactory.getInstance("X.509");
                    X509Certificate cert = (X509Certificate) cf.generateCertificate(bais);
                    jsseCerts = new X509Certificate[1];
                    jsseCerts[0] = cert;
                } catch (java.security.cert.CertificateException e) {
                    LOGGER.log(Level.SEVERE, "Certificate convertion failed", e);
                    return null;
                }

                setAttribute(SSLSupport.CERTIFICATE_KEY, jsseCerts);
                result = jsseCerts;
            }
        }

        return result;
    }


    private HttpRequestPacketImpl init() {
        cachedResponse.setRequest(this);
        setResponse(cachedResponse);
        return this;
    }

    @Override
    public ProcessingState getProcessingState() {
        return processingState;
    }

    public int getContentBytesRemaining() {
        return contentBytesRemaining;
    }

    public void setContentBytesRemaining(final int contentBytesRemaining) {
        this.contentBytesRemaining = contentBytesRemaining;
    }

    @Override
    protected void setExpectContent(boolean isExpectContent) {
        super.setExpectContent(isExpectContent);
    }

    @Override
    protected void setSecure(final boolean secure) {
        super.setSecure(secure);
    }


    @Override
    protected void reset() {
        processingState.recycle();
        contentBytesRemaining = -1;
        cachedResponse.recycle();
        
        super.reset();
    }

    @Override
    public void recycle() {
        reset();

        ThreadCache.putToCache(CACHE_IDX, this);
    }


}
