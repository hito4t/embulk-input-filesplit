package org.embulk.input.filesplit;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.embulk.config.ConfigSource;
import org.embulk.config.DataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class EmptyConfigSource implements ConfigSource
{

	@Override
	public <E> E get(Class<E> type, String attrName)
	{
		return null;
	}

	@Override
	public <E> E get(Class<E> type, String attrName, E defaultValue)
	{
		return defaultValue;
	}

	@Override
	public List<String> getAttributeNames()
	{
		return Collections.emptyList();
	}

	@Override
	public Iterable<Entry<String, JsonNode>> getAttributes() 
	{
		return Collections.emptyList();
	}

	@Override
	public ObjectNode getObjectNode()
	{
		return null;
	}

	@Override
	public boolean isEmpty() 
	{
		return true;
	}

	@Override
	public ConfigSource deepCopy()
	{
		return this;
	}

	@Override
	public ConfigSource getNested(String s) 
	{
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public ConfigSource getNestedOrSetEmpty(String s)
	{
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public <T> T loadConfig(Class<T> class1)
	{
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public ConfigSource merge(DataSource datasource)
	{
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public ConfigSource set(String s, Object obj)
	{
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public ConfigSource setAll(DataSource datasource)
	{
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public ConfigSource setNested(String s, DataSource datasource) 
	{
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}
	
}
