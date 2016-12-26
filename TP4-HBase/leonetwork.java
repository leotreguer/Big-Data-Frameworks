package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * Created by Léo on 03/11/2016
 */

public class leonetwork {

    // We initialize the configuration settings.
    // According to the POM file, we will use the original hbase-site.xml file
    private static Configuration conf = null;
    static{
        conf = HBaseConfiguration.create();
    }


    /**
     * To create a table
     */
    public static void createTable(String tableName, String[] families)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        // If the table already exists, we do nothing
        if (admin.tableExists(tableName)) {
            System.out.println("The table '"+tableName+"' already exists!");
        } else {
            //If it does not exist, we create it, with the tableName as HTableDescriptor
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            //We add the wanted columns to the table
            for (int i = 0; i < families.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(families[i]));
            }
            // Finally, we create the table
            admin.createTable(tableDesc);
            System.out.println("Table '" + tableName + "' created");
        }
    }

    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
            HTable table = new HTable(conf, tableName);
            // We initialize the scanner
            Scan s = new Scan();
            // We read the scanner
            ResultScanner ss = table.getScanner(s);
            // For each result, we get
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                    // print "row CF:property ts value"
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * To add a record to the table
     */

    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            // We create the RowKey
            Put put = new Put(Bytes.toBytes(rowKey));
            // We add the RowKey with the info
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recorded " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * main
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ArrayList<String> friendsArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        boolean c = true;

        String choice = "";
        String firstName;
        String yesNo;
        String value;
        Scanner sc = new Scanner(System.in);
        // We name the table as instructed
        String tableName = "ltreguer";
        String[] columnFamilies = {"info", "friends"};

        // We create the table Hbase
        leonetwork.createTable(tableName, columnFamilies);
        // We loop to add a person to the table (the social network)
        do {
            boolean d = true;
            // The name
            System.out.println("To add to the social network, please enter a person's name:");
            firstName = sc.nextLine();
            // The age
            System.out.println("Now, enter the person's age:");
            value = sc.nextLine();
            // We check that a number is entered
            while (!value.matches("\\d+")) {
                System.out.println("Please enter a number for the age ");
                value = sc.nextLine();
            }
            leonetwork.addRecord(tableName, firstName, "info", "age", value);
            // The gender
            System.out.println("Now, enter the person's gender (M or F) : ");
            value=sc.nextLine();
            // We check that a gender is entered
            while (value.charAt(0) != 'M' && value.charAt(0) != 'F') {
                System.out.println("Please enter the person's gender (M or F)");
                value = sc.nextLine();
            }
            leonetwork.addRecord(tableName, firstName, "info", "gender", value);
            // The BFF
            System.out.println("Who is the person's BFF ?");
            value = sc.nextLine();
            leonetwork.addRecord(tableName, firstName, "friends", "BFF", value);
            // Now, we add other friends of the person if needed
            // We loop as long as there are other friends to add
            do {
                System.out.println("Does the person have other friends ? y or n");
                yesNo = sc.nextLine();
                while (yesNo.charAt(0) != 'n' && yesNo.charAt(0) != 'y') {
                    System.out.println("I did not understand. y or n ?");
                    yesNo = sc.nextLine();
                }

                if(yesNo.charAt(0) == 'n') {
                    d = false;
                    break;
                }
                // We add the friends to an array
                System.out.println("What's the name of the person's friend ?");
                value = sc.nextLine();
                friendsArray.add(value);
            }while(d);
            // we convert the array into a string
            for(String s : friendsArray)
            {
                sb.append(s);
                sb.append(",");
            }

            leonetwork.addRecord(tableName, firstName, "friends", "others", sb.toString());
            //We empty the array of friends
            friendsArray.clear();
            sb.setLength(0);
            //We print the current table
            System.out.println("Current Social Network info :");
            leonetwork.getAllRecord(tableName);
            // We ask whether the user wants to add more friends
            System.out.println("Do you want to add a new person's info to the social network ? y or n");
            yesNo = sc.nextLine();
            while(yesNo.charAt(0) != 'n' && yesNo.charAt(0) != 'y')
            {
                System.out.println("I did not understand. y or n ?");
                yesNo = sc.nextLine();
            }

            if(yesNo.charAt(0) == 'n')
                c = false;

        }while(c);
        System.out.println("See you soon!");
    }
}