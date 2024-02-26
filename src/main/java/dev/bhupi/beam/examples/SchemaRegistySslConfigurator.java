package dev.bhupi.beam.examples;

import com.google.auto.service.AutoService;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(JvmInitializer.class)
public class SchemaRegistySslConfigurator implements JvmInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistySslConfigurator.class);
  private static final String TRUST_MANAGER_FACTORY_ALGORITHM = "PKIX";
  private static final String SSL_CONTEXT_PROTOCOL = "TLS";

  private static final String KEYSTORE_TYPE = "PKCS12";
  private static final String CERTIFICATE_FACTORY_TYPE = "X.509";

  private static X509Certificate[] getAcceptedIssuers(boolean useDefaultTrustManager)
      throws KeyStoreException, NoSuchAlgorithmException {
    if (useDefaultTrustManager) {
      TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(TRUST_MANAGER_FACTORY_ALGORITHM);
      trustManagerFactory.init(
          (KeyStore) null); // this will give us the default trust manager used by java
      return Arrays.stream(trustManagerFactory.getTrustManagers())
          .filter(trustManager -> trustManager instanceof X509TrustManager)
          .map(X509TrustManager.class::cast)
          .map(X509TrustManager::getAcceptedIssuers)
          .flatMap(Arrays::stream)
          .toArray(X509Certificate[]::new);
    } else {
      return new X509Certificate[] {};
    }
  }

  private static KeyStore createTrustStore(
      Certificate certificate, boolean withDefaultTrustManagers) {
    try {
      KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      keyStore.load(null, null);
      keyStore.setCertificateEntry("downloaded", certificate);
      X509Certificate[] acceptedIssuers = getAcceptedIssuers(withDefaultTrustManagers);
      for (int i = 0; i < acceptedIssuers.length; i += 1) {
        keyStore.setCertificateEntry(Integer.toString(i), acceptedIssuers[i]);
      }
      LOG.info(
          "added a certificate to a list of {} already accepted issuer(s)", acceptedIssuers.length);
      if (LOG.isDebugEnabled()) {
        List<Certificate> certificateList = new ArrayList<>(Arrays.asList(acceptedIssuers));
        certificateList.add(certificate);
        LOG.debug("certs in the truststore: {}", certificateList);
      }
      return keyStore;
    } catch (NoSuchAlgorithmException | KeyStoreException | IOException | CertificateException e) {
      throw new RuntimeException("unable to create a trust store", e);
    }
  }

  private static Certificate downloadCaCert(String caCertLocation) {
    LOG.info("downloading schema registry cacert from {}", caCertLocation);
    Storage storage = StorageOptions.getDefaultInstance().getService();
    try (PipedInputStream caCertInputStream = new PipedInputStream();
        PipedOutputStream caCertOutputStream = new PipedOutputStream(caCertInputStream)) {
      Thread downloadThread =
          new Thread(
              () -> storage.downloadTo(BlobId.fromGsUtilUri(caCertLocation), caCertOutputStream),
              "download thread");
      downloadThread.start();
      Certificate certificate =
          CertificateFactory.getInstance(CERTIFICATE_FACTORY_TYPE)
              .generateCertificate(caCertInputStream);
      downloadThread.join();
      return certificate;
    } catch (IOException | CertificateException | InterruptedException e) {
      throw new RuntimeException(
          String.format("unable to download the certificate from %s", caCertLocation), e);
    }
  }

  private static void initDefaultSslContext(KeyStore trustStore) {
    LOG.info("initializing a new default SSLContext with a truststore");
    try {
      TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(TRUST_MANAGER_FACTORY_ALGORITHM);
      trustManagerFactory.init(trustStore);
      SSLContext newDefaultSslContext = SSLContext.getInstance(SSL_CONTEXT_PROTOCOL);
      newDefaultSslContext.init(null, trustManagerFactory.getTrustManagers(), null);
      SSLContext.setDefault(newDefaultSslContext);
      HttpsURLConnection.setDefaultSSLSocketFactory(newDefaultSslContext.getSocketFactory());
    } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
      throw new RuntimeException("error while initializing default SSLContext", e);
    }
  }

  private static void addCertFrom(String caCertLocation) {
    Certificate certificate = downloadCaCert(caCertLocation);
    KeyStore trustStore = createTrustStore(certificate, true);
    initDefaultSslContext(trustStore);
  }

  @Override
  public void beforeProcessing(@NonNull PipelineOptions options) {
    addCertFrom(options.as(KafkaAvroExampleOptions.class).getSchemaRegistryCaCertLocation());
  }
}
