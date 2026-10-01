package ch.epfl.coshim.types;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocksMap<K, V> {
  private final Map<K, Chain<K, V>> locksMap;

  public LocksMap() {
    this.locksMap = new ConcurrentHashMap<>();
  }

  public Chain<K, V> getChain(K key) {
    return locksMap.get(key);
  }

  public void putChain(K key, Chain<K, V> chain) {
    locksMap.put(key, chain);
  }

  public boolean containsKey(K key) {
    return locksMap.containsKey(key);
  }
}
