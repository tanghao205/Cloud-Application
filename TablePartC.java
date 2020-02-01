import java.io.IOException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;

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

import org.apache.hadoop.hbase.util.Bytes;

public class TablePartC{

   public static void main(String[] args) throws IOException {

		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();
		// Instantiating HTable class
		HTable hTable = new HTable(config, "powers");
		
		String[] row = new String[6];

		try{
			
		FileReader fr = new FileReader("./input.csv");
        BufferedReader br = new BufferedReader(fr);		
		String line;
		
		while((line=br.readLine())!=null){   
			row=line.split(",");	
            line="";
				
			// Instantiating Put class
			// accepts a row name.
			Put p = new Put(Bytes.toBytes(row[0]));
			// adding values using add() method
			// accepts column family name, qualifier/row name ,value
			p.add(Bytes.toBytes("personal"),Bytes.toBytes("hero"),Bytes.toBytes(row[1]));
			p.add(Bytes.toBytes("personal"),Bytes.toBytes("power"),Bytes.toBytes(row[2]));
			p.add(Bytes.toBytes("professional"),Bytes.toBytes("name"),Bytes.toBytes(row[3]));
			p.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"),Bytes.toBytes(row[4]));
			p.add(Bytes.toBytes("custom"),Bytes.toBytes("color"),Bytes.toBytes(row[5]));
			// Saving the put Instance to the HTable.
			hTable.put(p);	
		}
		br.close();
		fr.close();	
		
        } catch (IOException e) {
            e.printStackTrace();
        }
		
		System.out.println("data inserted");
		// closing HTable
		hTable.close();   
		
   }
}

