package nmsl.versatile.mqtt;


public class DeviceInfo {
	String deviceName;
	boolean isAlive;
	int timeToLive;
	
	DeviceInfo(String deviceName, boolean isAlive, int timeToLive){
		this.deviceName = deviceName;
		this.isAlive = isAlive;
		this.timeToLive = timeToLive;
	}
	
	String genInsertQuery(String tableName) {
        String inTime   = new java.text.SimpleDateFormat("HHmmss").format(new java.util.Date());
		String query = "insert into " + tableName + " values ('" + deviceName + "', true, " + inTime + ");";
		return query;
	}
	//UPDATE tablename SET filedA='456' WHERE test='123' LIMIT 10;
	String genUpdateQuery(String tableName) {
		String inTime   = new java.text.SimpleDateFormat("HHmmss").format(new java.util.Date());
		String query = "UPDATE " + tableName +" SET timeToLive='"+inTime+"' WHERE deviceName='"+deviceName+"';";
		return query;
	}
	
	String genDeleteQuery(String tableName) {
		String query = "delete from " + tableName + " where deviceName='"+deviceName+"';";
		return query;
	}
}
