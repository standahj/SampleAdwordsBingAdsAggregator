package tools;

import java.util.Map;

public interface Collectable {
	public int collect(Map<String,String> arguments);
	public String getDescription();
	public void setDebug(boolean debug);
}
