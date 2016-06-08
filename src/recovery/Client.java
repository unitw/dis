package recovery;


public class Client implements Runnable{

	String _clientName;
	private static PersistenzManager _persistenzManager;

	public Client(String clientName)
	{
		this._clientName = clientName;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		PersistenzManager pm = _persistenzManager.getInstance();
		int taid = pm.beginTransaction();
		try {
			
			pm.write(taid,taid*10, this._clientName+"Client");
			pm.write(taid,taid*10, this._clientName+"Testen");
			pm.write(taid,taid*10+1, this._clientName+"Client");
			pm.write(taid,taid*10+2, this._clientName+"Client");
			pm.write(taid,taid*10+3, this._clientName+"Client");
			pm.commit(taid);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
