import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Get;


import org.apache.hadoop.hbase.util.Bytes;

public class TablePartD{

   public static void main(String[] args) throws IOException {

		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();
		// Instantiating HTable class
		HTable table = new HTable(config, "powers");
		// Instantiating Get class
		Get g = new Get(Bytes.toBytes("row1"));
		// Reading the data
		Result result = table.get(g);
		// Reading values from Result class object
		byte [] value1 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
		byte [] value2 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power"));
		byte [] value3 = result.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
		byte [] value4 = result.getValue(Bytes.toBytes("professional"),Bytes.toBytes("xp"));
		byte [] value5 = result.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));
		// Printing the values
		String hero = Bytes.toString(value1);
		String power = Bytes.toString(value2);
		String name = Bytes.toString(value3);
		String xp = Bytes.toString(value4);
		String color = Bytes.toString(value5);
		
		System.out.println("hero: " + hero + "," + " power: " + power + "," + " name: " + name + "," + " xp: " + xp + "," + " color: " + color);
		
		//"row 19"
		g = new Get(Bytes.toBytes("row19"));
		// Reading the data
		result = table.get(g);
		// Reading values from Result class object
		byte [] value21 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
		byte [] value25 = result.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));
		// Printing the values
		String hero2 = Bytes.toString(value21);
		String color2 = Bytes.toString(value25);
		
		System.out.println("hero: " + hero2 +  "," + " color: " + color2);
		
		//"row 1" again
		g = new Get(Bytes.toBytes("row1"));
		// Reading the data
		result = table.get(g);
		// Reading values from Result class object
		byte [] value31 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
		byte [] value33 = result.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
		byte [] value35 = result.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));
		// Printing the values
		String hero3 = Bytes.toString(value31);
		String name3 = Bytes.toString(value33);
		String color3 = Bytes.toString(value35);
		
		System.out.println("hero: " + hero3 + "," + " name: " + name3 + "," + " color: " + color3);
   
   }
}

