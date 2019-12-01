package org.apache.druid.segment.incremental;

import com.google.common.base.Supplier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.ISE;

public class InputRowContextExecutor
{
  private static final ThreadLocal<InputRow> in = new ThreadLocal<>();
  private final InputRow row;
  private final Runnable runnable;

  public InputRowContextExecutor(InputRow row, Runnable runnable)
  {
    this.row = row;
    this.runnable = runnable;
  }

  public void execute()
  {
    if (in.get() != null) {
      throw new ISE(String.format("Nesteed %s context not allowed.", InputRow.class.getSimpleName()));
    }
    in.set(row);
    try {
      runnable.run();
    } finally {
      in.remove();
    }

  }

  public static final Supplier<InputRow> getRowSupplier()
  {
    return in::get;
  }

}
