package LoggingandRecovery;


public class Main {

	/**
	 * @param args
	 * @throws Exception 
	 */
	
	/*
	 * wenn man Neue NutzDaten erstellen oder nach LogDaten recovery moechtet, 
	 * wird man jeweils code Kommentiert und auskommentiert
	 */
	public static void main(String[] args) throws Exception {
		ClientPool lauft = new ClientPool();
		new Thread(lauft.client1).start();
		new Thread(lauft.client2).start();
		new Thread(lauft.client3).start();
		new Thread(lauft.client4).start();
		new Thread(lauft.client5).start();
//		RecoveryWerkzeug rw = new RecoveryWerkzeug();
//		rw.crashRecovery();
	}
}
