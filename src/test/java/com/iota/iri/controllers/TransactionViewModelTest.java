package com.iota.iri.controllers;

import com.iota.iri.conf.Configuration;
import com.iota.iri.model.Hash;
import com.iota.iri.model.Transaction;
import com.iota.iri.storage.MemDBPersistenceProvider;
import com.iota.iri.storage.Tangle;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProviderTest;
import com.iota.iri.utils.Converter;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by paul on 3/5/17 for iri.
 */
public class TransactionViewModelTest {

    private static final TemporaryFolder dbFolder = new TemporaryFolder();
    private static final TemporaryFolder logFolder = new TemporaryFolder();

    private static final Random seed = new Random();

    @BeforeClass
    public static void setUp() throws Exception {
        dbFolder.create();
        logFolder.create();
        Configuration.put(Configuration.DefaultConfSettings.DB_PATH, dbFolder.getRoot().getAbsolutePath());
        Configuration.put(Configuration.DefaultConfSettings.DB_LOG_PATH, logFolder.getRoot().getAbsolutePath());
        Tangle.instance().addPersistenceProvider(RocksDBPersistenceProviderTest.rocksDBPersistenceProvider);
        Tangle.instance().init();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        Tangle.instance().shutdown();
        dbFolder.delete();
    }

    @Test
    public void getBundleTransactions() throws Exception {
    }

    @Test
    public void getBranchTransaction() throws Exception {
    }

    @Test
    public void getTrunkTransaction() throws Exception {
    }

    @Test
    public void getApprovers() throws Exception {
        TransactionViewModel transactionViewModel, otherTxVM, trunkTx, branchTx;


        int[] trits = getRandomTransactionTrits();
        trunkTx = new TransactionViewModel(trits, Hash.calculate(trits));

        branchTx = new TransactionViewModel(trits, Hash.calculate(trits));

        int[] childTx = getRandomTransactionTrits();
        System.arraycopy(trunkTx.getHash().trits(), 0, childTx, TransactionViewModel.TRUNK_TRANSACTION_TRINARY_OFFSET, TransactionViewModel.TRUNK_TRANSACTION_TRINARY_SIZE);
        System.arraycopy(branchTx.getHash().trits(), 0, childTx, TransactionViewModel.BRANCH_TRANSACTION_TRINARY_OFFSET, TransactionViewModel.BRANCH_TRANSACTION_TRINARY_SIZE);
        transactionViewModel = new TransactionViewModel(childTx, Hash.calculate(childTx));

        childTx = getRandomTransactionTrits();
        System.arraycopy(trunkTx.getHash().trits(), 0, childTx, TransactionViewModel.TRUNK_TRANSACTION_TRINARY_OFFSET, TransactionViewModel.TRUNK_TRANSACTION_TRINARY_SIZE);
        System.arraycopy(branchTx.getHash().trits(), 0, childTx, TransactionViewModel.BRANCH_TRANSACTION_TRINARY_OFFSET, TransactionViewModel.BRANCH_TRANSACTION_TRINARY_SIZE);
        otherTxVM = new TransactionViewModel(childTx, Hash.calculate(childTx));

        otherTxVM.store();
        transactionViewModel.store();
        trunkTx.store();
        branchTx.store();

        Set<Hash> approvers = trunkTx.getApprovers();
        assertNotEquals(approvers.size(), 0);
    }

    @Test
    public void fromHash() throws Exception {

    }

    @Test
    public void fromHash1() throws Exception {

    }

    @Test
    public void update() throws Exception {

    }

    @Test
    public void trits() throws Exception {
        /*
        int[] blanks = new int[13];
        for(int i=0; i++ < 1000;) {
            int[] trits = getRandomTransactionTrits(seed), searchTrits;
            System.arraycopy(new int[TransactionViewModel.VALUE_TRINARY_SIZE], 0, trits, TransactionViewModel.VALUE_TRINARY_OFFSET, TransactionViewModel.VALUE_TRINARY_SIZE);
            Converter.copyTrits(seed.nextLong(), trits, TransactionViewModel.VALUE_TRINARY_OFFSET, TransactionViewModel.VALUE_USABLE_TRINARY_SIZE);
            System.arraycopy(blanks, 0, trits, TransactionViewModel.TRUNK_TRANSACTION_TRINARY_OFFSET-blanks.length, blanks.length);
            System.arraycopy(blanks, 0, trits, TransactionViewModel.BRANCH_TRANSACTION_TRINARY_OFFSET-blanks.length, blanks.length);
            System.arraycopy(blanks, 0, trits, TransactionViewModel.BRANCH_TRANSACTION_TRINARY_OFFSET + TransactionViewModel.BRANCH_TRANSACTION_TRINARY_SIZE-blanks.length, blanks.length);
            TransactionViewModel transactionViewModel = new TransactionViewModel(trits);
            transactionViewModel.store();
            assertArrayEquals(transactionViewModel.trits(), TransactionViewModel.fromHash(transactionViewModel.getHash()).trits());
        }
        */
    }

    @Test
    public void getBytes() throws Exception {
        /*
        for(int i=0; i++ < 1000;) {
            int[] trits = getRandomTransactionTrits(seed);
            System.arraycopy(new int[TransactionViewModel.VALUE_TRINARY_SIZE], 0, trits, TransactionViewModel.VALUE_TRINARY_OFFSET, TransactionViewModel.VALUE_TRINARY_SIZE);
            Converter.copyTrits(seed.nextLong(), trits, TransactionViewModel.VALUE_TRINARY_OFFSET, TransactionViewModel.VALUE_USABLE_TRINARY_SIZE);
            TransactionViewModel transactionViewModel = new TransactionViewModel(trits);
            transactionViewModel.store();
            assertArrayEquals(transactionViewModel.getBytes(), TransactionViewModel.fromHash(transactionViewModel.getHash()).getBytes());
        }
        */
    }

    @Test
    public void getHash() throws Exception {

    }

    @Test
    public void getAddress() throws Exception {

    }

    @Test
    public void getTag() throws Exception {

    }

    @Test
    public void getBundleHash() throws Exception {

    }

    @Test
    public void getTrunkTransactionHash() throws Exception {
    }

    @Test
    public void getBranchTransactionHash() throws Exception {

    }

    @Test
    public void getValue() throws Exception {

    }

    @Test
    public void value() throws Exception {

    }

    @Test
    public void setValidity() throws Exception {

    }

    @Test
    public void getValidity() throws Exception {

    }

    @Test
    public void getCurrentIndex() throws Exception {

    }

    @Test
    public void getLastIndex() throws Exception {

    }

    @Test
    public void mightExist() throws Exception {

    }

    @Test
    public void update1() throws Exception {

    }

    @Test
    public void setAnalyzed() throws Exception {

    }


    @Test
    public void dump() throws Exception {

    }

    @Test
    public void store() throws Exception {

    }

    @Test
    public void updateTips() throws Exception {

    }

    @Test
    public void updateReceivedTransactionCount() throws Exception {

    }

    @Test
    public void updateApprovers() throws Exception {

    }

    @Test
    public void hashesFromQuery() throws Exception {

    }

    @Test
    public void approversFromHash() throws Exception {

    }

    @Test
    public void fromTag() throws Exception {

    }

    @Test
    public void fromBundle() throws Exception {

    }

    @Test
    public void fromAddress() throws Exception {

    }

    @Test
    public void getTransactionAnalyzedFlag() throws Exception {

    }

    @Test
    public void getType() throws Exception {

    }

    @Test
    public void setArrivalTime() throws Exception {

    }

    @Test
    public void getArrivalTime() throws Exception {

    }

    @Test
    public void updateHeightShouldWork() throws Exception {
        int count = 4;
        TransactionViewModel[] transactionViewModels = new TransactionViewModel[count];
        Hash hash = getRandomTransactionHash();
        transactionViewModels[0] = new TransactionViewModel(getRandomTransactionWithTrunkAndBranch(Hash.NULL_HASH,
                Hash.NULL_HASH), hash);
        transactionViewModels[0].store();
        for(int i = 0; ++i < count; ) {
            transactionViewModels[i] = new TransactionViewModel(getRandomTransactionWithTrunkAndBranch(hash,
                    Hash.NULL_HASH), hash = getRandomTransactionHash());
            transactionViewModels[i].store();
        }

        transactionViewModels[count-1].updateHeights();

        for(int i = count; i > 1; ) {
            assertEquals(i, TransactionViewModel.fromHash(transactionViewModels[--i].getHash()).getHeight());
        }
    }

    @Test
    public void updateHeightPrefilledSlotShouldFail() throws Exception {
        int count = 4;
        TransactionViewModel[] transactionViewModels = new TransactionViewModel[count];
        Hash hash = getRandomTransactionHash();
        for(int i = 0; ++i < count; ) {
            transactionViewModels[i] = new TransactionViewModel(getRandomTransactionWithTrunkAndBranch(hash,
                    Hash.NULL_HASH), hash = getRandomTransactionHash());
            transactionViewModels[i].store();
        }

        transactionViewModels[count-1].updateHeights();

        for(int i = count; i > 1; ) {
            assertEquals(0, TransactionViewModel.fromHash(transactionViewModels[--i].getHash()).getHeight());
        }
    }

    @Test
    public void findShouldBeSuccessful() throws Exception {
        int[] trits = getRandomTransactionTrits();
        TransactionViewModel transactionViewModel = new TransactionViewModel(trits, Hash.calculate(trits));
        transactionViewModel.store();
        Hash hash = transactionViewModel.getHash();
        Assert.assertArrayEquals(TransactionViewModel.find(Arrays.copyOf(hash.bytes(), TransactionRequester.REQUEST_HASH_SIZE)).getBytes(), transactionViewModel.getBytes());
    }

    @Test
    public void findShoultReturnNull() throws Exception {
        int[] trits = getRandomTransactionTrits();
        TransactionViewModel transactionViewModel = new TransactionViewModel(trits, Hash.calculate(trits));
        trits = getRandomTransactionTrits();
        TransactionViewModel transactionViewModelNoSave = new TransactionViewModel(trits, Hash.calculate(trits));
        transactionViewModel.store();
        Hash hash = transactionViewModelNoSave.getHash();
        Assert.assertFalse(Arrays.equals(TransactionViewModel.find(Arrays.copyOf(hash.bytes(), TransactionRequester.REQUEST_HASH_SIZE)).getBytes(), transactionViewModel.getBytes()));
    }

    private Transaction getRandomTransaction(Random seed) {
        Transaction transaction = new Transaction();
        transaction.bytes = Converter.bytes(Arrays.stream(new int[TransactionViewModel.SIGNATURE_MESSAGE_FRAGMENT_TRINARY_SIZE]).map(i -> seed.nextInt(3)-1).toArray());
        return transaction;
    }
    public static int[] getRandomTransactionWithTrunkAndBranch(Hash trunk, Hash branch) {
        int[] trits = getRandomTransactionTrits();
        System.arraycopy(trunk.trits(), 0, trits, TransactionViewModel.TRUNK_TRANSACTION_TRINARY_OFFSET,
                TransactionViewModel.TRUNK_TRANSACTION_TRINARY_SIZE);
        System.arraycopy(branch.trits(), 0, trits, TransactionViewModel.BRANCH_TRANSACTION_TRINARY_OFFSET,
                TransactionViewModel.BRANCH_TRANSACTION_TRINARY_SIZE);
        return trits;
    }
    public static int[] getRandomTransactionTrits() {
        return Arrays.stream(new int[TransactionViewModel.TRINARY_SIZE]).map(i -> seed.nextInt(3)-1).toArray();
    }
    public static Hash getRandomTransactionHash() {
        return new Hash(Arrays.stream(new int[Hash.SIZE_IN_TRITS]).map(i -> seed.nextInt(3)-1).toArray());
    }
}