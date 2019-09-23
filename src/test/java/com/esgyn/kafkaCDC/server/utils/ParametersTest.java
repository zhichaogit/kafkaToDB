package com.esgyn.kafkaCDC.server.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class ParametersTest {
    Parameters paras = null;
    @Before
    public void setUp() throws Exception {
        paras = new Parameters();
    }

    @Test
    public void getPartArrayFromStrTest1() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        int[] expecteds= {0,1,2,3};
        // Call private method,Accessible must true.
        Method declaredMethod = paras.getClass().getDeclaredMethod("getPartArrayFromStr", String.class);
        declaredMethod.setAccessible(true);
        
        int[] actuals = (int[])declaredMethod.invoke(paras, "4");
        assertArrayEquals(expecteds, actuals);
    }
    
    @Test
    public void getPartArrayFromStrTest2() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        int[] expecteds= {1,4,5,8};
        // Call private method,Accessible must true.
        Method declaredMethod = paras.getClass().getDeclaredMethod("getPartArrayFromStr", String.class);
        declaredMethod.setAccessible(true);
        
        int[] actuals = (int[])declaredMethod.invoke(paras, "1,4-5,8");
        assertArrayEquals(expecteds, actuals);
    }
    
    @Test
    public void getPartArrayFromStrTest3() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        int[] expecteds= {2};
        // Call private method,Accessible must true.
        Method declaredMethod = paras.getClass().getDeclaredMethod("getPartArrayFromStr", String.class);
        declaredMethod.setAccessible(true);
        
        int[] actuals = (int[])declaredMethod.invoke(paras, "2-2");
        assertArrayEquals(expecteds, actuals);
    }

    @Test
    public void getPartArrayFromStrTest4() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // Call private method,Accessible must true.
        Method declaredMethod = paras.getClass().getDeclaredMethod("getPartArrayFromStr", String.class);
        declaredMethod.setAccessible(true);

        int[] actuals = (int[])declaredMethod.invoke(paras, "-1");
        assertNull(actuals);
    }

}
