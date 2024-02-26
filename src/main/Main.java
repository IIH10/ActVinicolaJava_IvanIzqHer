package main;

import manager.Manager;

public class Main {

	public static void main(String[] args) {
		Manager manager = Manager.getInstance();
		manager.init();
	}

}
