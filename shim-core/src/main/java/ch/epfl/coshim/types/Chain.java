package ch.epfl.coshim.types;

public class Chain<K, V> {
  private LockNode<K, V> head;
  private final Object lock = new Object();

  public Chain() {
    this.head = null;
  }

  public LockNode<K, V> getHead() {
    return head;
  }

  public void setHead(LockNode<K, V> head) {
    this.head = head;
  }

  public boolean isEmpty() {
    return head == null;
  }

  public LockNode<K, V> search(K key) {
    synchronized (lock) {
      LockNode<K, V> current = head;
      while (current != null) {
        if (current.getKey().equals(key)) {
          return current;
        }
        current = current.getNext();
      }
      return null;
    }
  }

  private void appendNoSync(LockNode<K, V> newNode) {
    if (isEmpty()) {
      head = newNode;
    } else {
      LockNode<K, V> current = head;
      while (current.getNext() != null) {
        current = current.getNext();
      }
      current.setNext(newNode);
      newNode.setPrev(current);
    }
  }

  public void append(LockNode<K, V> newNode) {
    synchronized (lock) {
      appendNoSync(newNode);
    }
  }

  private boolean removeNoSync(K key) {
    if (isEmpty()) {
      return false;
    }
    if (head.getKey().equals(key)) {
      LockNode<K, V> temp = head.getNext();
      head = temp;
      temp.setPrev(null);
      return true;
    }
    LockNode<K, V> current = head;
    while (current != null) {
      if (current.getKey().equals(key)) {
        LockNode<K, V> prevNode = current.getPrev();
        LockNode<K, V> nextNode = current.getNext();
        if (prevNode != null) {
          prevNode.setNext(nextNode);
        }
        if (nextNode != null) {
          nextNode.setPrev(prevNode);
        }
        return true;
      }
      current = current.getNext();
    }
    return false;
  }

  public boolean remove(K key) {
    synchronized (lock) {
      return removeNoSync(key);
    }
  }

  public boolean upgradeNode(LockNode<K, V> node, Transaction<K, V> transaction) {
    synchronized (lock) {
      LockNode<K, V> current = head;
      while (current.getNext() != null && current.getNext().getMode() == LockType.LockMode.SHARED) {
        current = current.getNext();
      }
      LockNode<K, V> nextNode = current.getNext();
      if (nextNode != null && nextNode.isUpgraded()) return false;
      else if (current == node
          && node.getTransactionIds().contains(transaction)
          && node.getTransactionIds().size() == 1) {
        node.setUpgraded(true);
        node.setMode(LockType.LockMode.EXCLUSIVE);
        K key = node.getKey();
        transaction.addLock(key, new LockType<>(LockType.LockMode.EXCLUSIVE, key));
        return true;
      }
      node.getTransactionIds().remove(transaction);
      LockNode<K, V> newNode = new LockNode<>(node.getKey(), LockType.LockMode.EXCLUSIVE);
      newNode.addTransactionId(transaction);
      newNode.setUpgraded(true);
      newNode.setPrev(current);
      newNode.setNext(nextNode);
      if (nextNode != null) {
        nextNode.setPrev(newNode);
      } else {
        appendNoSync(newNode);
      }
      if (node.getTransactionIds().isEmpty()) {
        if (node.getPrev() != null) {
          node.getPrev().setNext(node.getNext());
        }
        if (node.getNext() != null) {
          node.getNext().setPrev(node.getPrev());
        }
        removeNoSync(node.getKey());
      }
      return true;
    }
  }
}
