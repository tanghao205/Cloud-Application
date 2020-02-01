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

import org.apache.hadoop.hbase.util.Bytes;

public class TablePartA{

   public static void main(String[] args) throws IOException {

		// Instantiating configuration class
		Configuration con = HBaseConfiguration.create();
		// Instantiating HbaseAdmin class
		HBaseAdmin admin = new HBaseAdmin(con);
		
		//create table "powers"
		// Instantiating table descriptor class
		HTableDescriptor tableDescriptorPowers = new
		HTableDescriptor(TableName.valueOf("powers"));
		// Adding column families to table descriptor
		tableDescriptorPowers.addFamily(new HColumnDescriptor("personal"));
		tableDescriptorPowers.addFamily(new HColumnDescriptor("professional"));
		tableDescriptorPowers.addFamily(new HColumnDescriptor("custom"));
		// Execute the table through admin
		admin.createTable(tableDescriptorPowers);
		// System.out.println(" Table powers created ");
		
		//create table "food"
		// Instantiating table descriptor class
		HTableDescriptor tableDescriptorFood = new
		HTableDescriptor(TableName.valueOf("food"));
		// Adding column families to table descriptor
		tableDescriptorFood.addFamily(new HColumnDescriptor("nutrition"));
		tableDescriptorFood.addFamily(new HColumnDescriptor("taste"));
		// Execute the table through admin
		admin.createTable(tableDescriptorFood);
		// System.out.println(" Table food created ");
	
   }
}

