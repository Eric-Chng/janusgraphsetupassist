package org.janusgraph.graphdb.application;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;

import java.util.concurrent.ExecutionException;

public class FDBTransactionCheck {
    public static void main(String[] args) {
        FDB fdb = FDB.selectAPIVersion(620);

        byte[] key1 = Tuple.from("key1").pack();
        byte[] val1 = Tuple.from("val1").pack();

        byte[] key2 = Tuple.from("key2").pack();
        byte[] val2 = Tuple.from("val2").pack();

        try (Database db = fdb.open()) {
            Transaction tx = db.createTransaction();

            tx.set(key1, val1);
            tx.set(key2, val2);
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Try to read values
        try (Database db = fdb.open()) {
            Transaction tx = db.createTransaction();

            String v1 = Tuple.fromBytes(tx.get(key1).get()).getString(0);
            System.out.println(v1);

            Thread.sleep(10_000);

            String v2 = Tuple.fromBytes(tx.get(key2).get()).getString(0);
            System.out.println(v2);

            tx.commit();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
