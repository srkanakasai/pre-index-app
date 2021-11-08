package com.smarsh.model;

public class Pair<T, V> {
	public T left;
	public V right;

	public Pair(T left, V right) {
		this.left = left;
		this.right = right;
	}

	public V getRight() {
		return right;
	}

	public T getLeft() {
		return left;
	}

	public void setLeft(T left) {
		this.left = left;
	}

	public void setRight(V right) {
		this.right = right;
	}
}
