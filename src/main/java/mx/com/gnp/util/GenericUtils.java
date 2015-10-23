package mx.com.gnp.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GenericUtils {
	
	
	public static final Date convertStringToDate(String dateString){
		Date date=null;
		if(dateString!=null && dateString.length()>0 && dateString.length()<9 && !dateString.equals("0") && !dateString.equals("null")){
			DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			try {
				date = formatter.parse(dateString);
			} catch (ParseException e) {
				e.printStackTrace();
				return date;
			}
		}		
		return date;
	}
	
	public static final String convertDateToString(Date date){
		String dateString=null;
		DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		if(date!=null){
			dateString = formatter.format(date);
		}
		return dateString;
	}
	
	@SuppressWarnings("deprecation")
	public static Long getDifferenceDays(Date date1, Date date2) {
		
		Long days = 0L;
		int year = date1.getYear();
		if(year==69) return 0L;
		
		year = date2.getYear();
		if(year==69) return 0L;
		
	    long diffInMillies = date2.getTime() - date1.getTime();
	    List<TimeUnit> units = new ArrayList<TimeUnit>(EnumSet.allOf(TimeUnit.class));
	    Collections.reverse(units);
	    Map<TimeUnit,Long> result = new LinkedHashMap<TimeUnit,Long>();
	    long milliesRest = diffInMillies;
	    for ( TimeUnit unit : units ) {
	        long diff = unit.convert(milliesRest,TimeUnit.MILLISECONDS);
	        long diffInMilliesForUnit = unit.toMillis(diff);
	        milliesRest = milliesRest - diffInMilliesForUnit;
	        result.put(unit,diff);
	    }
	    
	    if(result!=null) days= result.get(TimeUnit.DAYS);
	    return days;
	}

}
