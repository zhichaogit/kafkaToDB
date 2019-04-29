package server;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.esgyn.kafkaCDC.server.KafkaCDC;

public class KafkaCDCTest {
    KafkaCDC kafkaCDC=null;
    @Before
    public void init() throws Exception {
        kafkaCDC = new KafkaCDC();
    }
    @Test
    public void isValidLong() throws Exception {
        assertTrue(kafkaCDC.isValidLong("996"));
    }
    
    @Test
    public void isValidLong2() throws Exception {
        assertFalse(kafkaCDC.isValidLong("aaa996ccc"));
    }
    
    @Test
    public void getNotExistPartsTest() throws Exception {
       
        int[] partsArr= {0,1,2,3,4};
        int[] existPartsArr= {0,1,2,3,4,5,6,7,8,9};
        List notExistParts = kafkaCDC.getNotExistParts(partsArr, existPartsArr);
        
        assertTrue(notExistParts.size()==0);
    }

    @Test
    public void getNotExistPartsTest2() throws Exception {
     
        int[] partsArr= {0,1,2,3,4,5,6,7,8,9,10,11};
        int[] existPartsArr= {0,1,2,3,4,5,6,7,8,9};
        List notExistParts = kafkaCDC.getNotExistParts(partsArr, existPartsArr);
        assertFalse(notExistParts.size()==0); 
    }
}
