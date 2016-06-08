package recovery;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class PersistenzManager {

    /*
	 *  Detail von PesistenzManager wird noch nicht bestimmt
     */
    private static volatile PersistenzManager _instance;
    private static File _NUTZ_DATEN = null;
    private static File _LOG_DATEN = null;
    private String _toWrite;
    private Hashtable<String, List<String>> _puffer = new Hashtable<String, List<String>>();
    private AtomicInteger _counter = new AtomicInteger(1);
    private AtomicInteger _logSNCounter = new AtomicInteger(1);

    private PersistenzManager() {
        _NUTZ_DATEN = new File(
                "data.txt");
        _LOG_DATEN = new File(
                "log.txt");
    }

    public static PersistenzManager getInstance() {
        if (_instance == null) {
            synchronized (PersistenzManager.class) {
                if (_instance == null) {
                    _instance = new PersistenzManager();
                }
            }
        }
        return _instance;
    }

    public int beginTransaction() {
        int taid;
        boolean flag;
        do {
            taid = _counter.get();
            flag = _counter.compareAndSet(taid, taid + 1);
        } while (!flag);
        return taid;
    }

    /*
	 * zu Puffer auschreiben, wenn die Menge von Daten mehr als 5 Transaktionen, 
	 * werden sie automatisch zu Databases ausschreiben.
     */
    public synchronized void write(int taid, int pageid, String data) throws Exception {
        List<String> pufferTADaten = new ArrayList<String>();
        int logSN = this.getLogSN();
        String strLogSN = null;
        if (logSN < 10) {
            strLogSN = "0" + logSN;
        } else {
            strLogSN = logSN + "";
        }
        _toWrite = pageid + "," + strLogSN + "," + data;
        System.out.println(_toWrite);
        //Falls Transaktions ID exsistiert schon in der Puffer. 
        if (this._puffer.containsKey(taid + "")) {
            pufferTADaten = this._puffer.remove(taid + "");
            int commaPosition = this._toWrite.indexOf(",");
            //Falls in der taid Transaktion, gibt es auch gleich Pageid,
            //vorbereiten fuer ueberschreiben
            Boolean flag = false;
            for (int i = 0; i < pufferTADaten.size(); i++) {
                String altereVersion = pufferTADaten.get(i);
                String extrtPageid = altereVersion.substring(0, commaPosition);
                if (extrtPageid.equalsIgnoreCase(pageid + "")) {
                    int indexOfAltereVersion = pufferTADaten.indexOf(altereVersion);
                    pufferTADaten.remove(altereVersion);
                    pufferTADaten.add(indexOfAltereVersion, _toWrite);
                    flag = true;
                }
            }
            if (!flag) {
                pufferTADaten.add(_toWrite);
            }
        } else {
            pufferTADaten.add(_toWrite);
        }

        this._puffer.put(taid + "", pufferTADaten);
//		if (!rcv) 
        this.logDaten(pageid, taid, logSN);
//		else this.schreibenAusPufferPool(taid+"");// Falls es recovery ist, dann einfach von Puffer zu Databases auschreiben

        int anzahlVonSeiten = this.anzahlSeiten();
        //Falls Menge der Puffer mehr als 5, wird das Programm darunter durchgefuehrt. 
        if (anzahlVonSeiten >= 5) {
            Set<String> taidSet = this._puffer.keySet();
            List<String> taidList = new ArrayList<String>(taidSet);
            for (int i = 0; i < taidList.size(); i++) {
                String key = taidList.get(i);
                if (key.contains("fertig")) {
                    this.schreibenAusPufferPool(key); //(Integer.valueOf(sauberIntKey));
                }
            }
        }
    }

    /*
	 * von Puffer zu Databases ausschreiben
     */
    public synchronized void schreibenAusPufferPool(String taid) {
        try {
            if (this._puffer.containsKey(taid)) {
                List<String> TADaten = this._puffer.remove(taid);
                String einCommit = TADaten.get(0);
                int commaPosition = einCommit.indexOf(",");
                //long len = einCommit.length() + 1000; //sieht kommisch aus 
                RandomAccessFile raf = new RandomAccessFile(_NUTZ_DATEN, "rw");
                for (int i = 0; i < TADaten.size(); i++) {
                    String einzelneCommit = TADaten.get(i);
                    einzelneCommit = einzelneCommit + ";" + "\n";
                    long len = einzelneCommit.length();//sieht kommisch aus 
                    int pageID = Integer.valueOf(einzelneCommit.substring(0, commaPosition));
                    raf.seek((pageID - 1) * len);//((pageID)*(len+3));
                    raf.writeBytes(einzelneCommit);
                }
                raf.close();
            } else {
                System.out.println("Es gibt nicht diesen Schluessel ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public synchronized void commit(int taid) throws Exception {
        List<String> TAofTaid = this._puffer.remove(taid + "");
        this._puffer.put("fertig" + taid, TAofTaid);
        int logSN = this.getLogSN();
        String strLogSN = null;
        if (logSN < 10) {
            strLogSN = "0" + logSN;
        } else {
            strLogSN = logSN + "";
        }
        String logFormat = strLogSN + "," + taid + "," + "committeds" + ";" + "\n";
        long len = logFormat.length();// +200;
        System.out.println(logFormat);
        RandomAccessFile raf = new RandomAccessFile(this._LOG_DATEN, "rw");
        raf.seek(Integer.valueOf(logSN - 1) * len);//*(len+2));
        raf.writeBytes(logFormat);
        raf.close();
        if (this._puffer.size() == 1) {
            this.schreibenAusPufferPool("fertig" + taid);
        }
    }

    private void logDaten(int pageid, int taid, int logSN) throws IOException {
        //TODO 
        String strLogSN = null;
        if (logSN < 10) {
            strLogSN = "0" + logSN;
        } else {
            strLogSN = logSN + "";
        }
        int lastCommaPosition = this._toWrite.lastIndexOf(",");
        String dataSatz = this._toWrite.substring(lastCommaPosition + 1);
        String logFormat = strLogSN + "," + taid + "," + pageid + "," + dataSatz + ";" + "\n";
        long len = logFormat.length(); // +200;
        RandomAccessFile raf = new RandomAccessFile(this._LOG_DATEN, "rw");
        raf.seek(Integer.valueOf(logSN - 1) * len);//(len+2));
        raf.writeBytes(logFormat);
        raf.close();
    }

    private int getLogSN() {
        int logSN;
        boolean flag;
        do {
            logSN = this._logSNCounter.get();
            flag = _logSNCounter.compareAndSet(logSN, logSN + 1);
        } while (!flag);
        return logSN;
    }

    private int anzahlSeiten() {
        if (this._puffer.isEmpty()) {
            return 0;
        } else {
            Set<String> taidSet = this._puffer.keySet();
            List<String> taidList = new ArrayList<String>(taidSet);
            int result = 0;
            for (int i = 0; i < taidList.size(); i++) {
                result = result + this._puffer.get(taidList.get(i)).size();
            }
            return result;
        }
    }

    /**
     * @return the _NUTZ_DATEN
     */
    public static File get_NUTZ_DATEN() {
        return _NUTZ_DATEN;
    }
}
