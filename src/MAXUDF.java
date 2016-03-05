package com.hadoop.pig1;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;


public class MAXUDF extends EvalFunc<Integer> {

    @Override
    public Integer exec(Tuple input)  {
         
            try {
    
				return max(input);
			} catch (ExecException e) {
			
				e.printStackTrace();
			}
			return null;
        
    }
     protected Integer max(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        
        if(values.size() == 0) {
    
            return null;
        }

        Integer curMax = Integer.MIN_VALUE;
        boolean checkNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
        	
            Tuple t = it.next();
            
            try {
                Integer intdata = (Integer)t.get(0);
           
                Integer d = intdata != null ? Integer.valueOf(intdata.toString()) : null;
                if (d == null) continue;
                checkNonNull = true;
                curMax = java.lang.Math.max(curMax, d);
            } catch (RuntimeException exp) {
       
            }
        }

        if(checkNonNull) {
            return new Integer(curMax);
        } else {
            return null;
        }
    }
    
        
   }
