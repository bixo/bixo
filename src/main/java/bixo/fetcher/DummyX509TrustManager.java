package bixo.fetcher;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

public class DummyX509TrustManager implements X509TrustManager {
    private X509TrustManager standardTrustManager = null;

    /**
     * Constructor for DummyX509TrustManager.
     */
    public DummyX509TrustManager(KeyStore keystore) throws NoSuchAlgorithmException, KeyStoreException {
        super();
        String algo = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory factory = TrustManagerFactory.getInstance(algo);
        factory.init(keystore);
        TrustManager[] trustmanagers = factory.getTrustManagers();
        if (trustmanagers.length == 0) {
            throw new NoSuchAlgorithmException(algo + " trust manager not supported");
        }
        this.standardTrustManager = (X509TrustManager)trustmanagers[0];
    }

    /**
     * @see javax.net.ssl.X509TrustManager#checkClientTrusted(X509Certificate[], String)
     */
    public boolean isClientTrusted(X509Certificate[] certificates) {
        return true;
    }

    /**
     * @see javax.net.ssl.X509TrustManager#checkServerTrusted(X509Certificate[], String)
     */
    public boolean isServerTrusted(X509Certificate[] certificates) {
        return true;
    }

    /**
     * @see javax.net.ssl.X509TrustManager#getAcceptedIssuers()
     */
    public X509Certificate[] getAcceptedIssuers() {
        return this.standardTrustManager.getAcceptedIssuers();
    }

    public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
        // do nothing

    }

    public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
        // do nothing

    }
}
