package org.opendatakraken.dblibrary;

public interface DBLibrary {

	public String getName();
	
	public String[] getModules();

	public String[] getObjects();

	public void install();

}
