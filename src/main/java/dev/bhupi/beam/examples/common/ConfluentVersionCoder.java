package dev.bhupi.beam.examples.common;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ConfluentVersionCoder extends AtomicCoder<ConfluentVersion> {

  private final StringUtf8Coder stringUtf8Coder = StringUtf8Coder.of();
  private final VarIntCoder varIntCoder = VarIntCoder.of();

  private static class LazyInit {
    private static final ConfluentVersionCoder INSTANCE = new ConfluentVersionCoder();
  }

  ConfluentVersionCoder() {}

  public static ConfluentVersionCoder of() {
    return LazyInit.INSTANCE;
  }

  @Override
  public void encode(ConfluentVersion value, @NonNull OutputStream outStream) throws IOException {
    Preconditions.checkNotNull(value, "ConfluentVersion is null");
    stringUtf8Coder.encode(value.getTopic(), outStream);
    stringUtf8Coder.encode(value.getSubject(), outStream);
    varIntCoder.encode(value.getVersion(), outStream);
  }

  @Override
  public ConfluentVersion decode(@NonNull InputStream inStream) throws IOException {
    String topic = stringUtf8Coder.decode(inStream);
    String subject = stringUtf8Coder.decode(inStream);
    Integer version = varIntCoder.decode(inStream);
    return ConfluentVersion.of(topic, subject, version);
  }
}
