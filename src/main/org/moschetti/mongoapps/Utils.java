/**
 *  A place to hold some funcs shared by the underlying JSON parser
 *  and the main mongomtimport logic.
 *  THis imp is MUCH faster than javax.xml.datatypeconvert or whatever the heck 
 *  the "normal easy" way to do this is....
 */
import java.util.Calendar;
import java.util.Date;

class Utils {

    // buildNumber(s, 0, 4);
    static int buildNumber(String s, int idx, int len) {
	int item = 0;

	// k is going 0 to len-1. i.e. 0 to 3 for year...
	for(int k = 0; k < len; k++) {

	    // Given 2013, jump to 3 
	    int nx = idx + (len-1) - k;

	    char c = s.charAt(nx);
	    switch(k) {
	    case 3:
		item += (c - '0') * 1000;
		break;
	    case 2:
		item += (c - '0') * 100;
		break;
	    case 1:
		item += (c - '0') * 10;
		break;
	    case 0:
		item += (c - '0');
		break;
	    }
	}

	return item;
    }


    /**
     *  2014-06-23T09:26:26.214-0400    extended millis spec
     *  2014-06-23T09:26:26.214         extended millis spec
     *  2014-06-23T09:26:26.214Z        extended millis spec w/Z
     *  2014-06-23T09:26:26-0400        "standard" ISO8601 spec
     *  2014-06-23T09:26:26             assumes GMT+0
     *  2014-06-23T09:26:26Z            explicit GMT+0
     *
     *  0123456789012345678901234567890
     *            1         2     
     */
    static Date cvtISODateToDate(String s) {
	int year = 0;
	int month = 0;
	int day = 0;
	
	int hour = 0;
	int min = 0;
	int sec = 0;
	int msec = 0;
	int tzhrs = -1; // i.e. unset
	int tzmin = -1; // i.e. unset

	int mult = 1;

	int slen = s.length();
	
	year  = buildNumber(s, 0, 4);
	month = buildNumber(s, 5, 2);
	day   = buildNumber(s, 8, 2);
	hour  = buildNumber(s, 11, 2);
	min   = buildNumber(s, 14, 2);
	sec   = buildNumber(s, 17, 2);


	if(slen == 28) {
	    msec = buildNumber(s, 20, 3);	
	    tzhrs = buildNumber(s, 24, 2);
	    tzmin = buildNumber(s, 26, 2);
	    if(s.charAt(23) == '+') {
		mult = -1;
	    }

	} else if(slen == 23) {
	    //  2014-06-23T09:26:26.214         extended millis spec w/Z
	    msec = buildNumber(s, 20, 3);	

	} else if(slen == 24) {
	    if(s.charAt(19) == '.') { //  2014-06-23T09:26:26.214Z        extended millis spec w/Z
		msec = buildNumber(s, 20, 3);	
		tzhrs = 0; // Z means assume 00:00 offset
		tzmin = 0;
	    } else { 
		//  2014-06-23T09:26:26-0400        "standard" ISO8601 spec
		tzhrs = buildNumber(s, 20, 2);
		tzmin = buildNumber(s, 22, 2);
		if(s.charAt(19) == '+') {
		    mult = -1;
		}		
	    }

	} else if(slen == 20 && s.charAt(19) == 'Z') {
	    tzhrs = 0; // Z means assume 00:00 offset
	    tzmin = 0;
	}


	Calendar cal = Calendar.getInstance();
	cal.clear();

	// If we explicitly set up GMT offset, then FORCE the Calendar
	// object into GMT mode.   Otherwise, when we do cal.getTime() later
	// the local time JVM offset will automagically be added in!  Yuck
	if(tzhrs != -1 || tzmin != -1) {
	    cal.setTimeZone(java.util.TimeZone.getTimeZone("GMT"));
	}

	cal.set( Calendar.YEAR,  year );
	cal.set( Calendar.MONTH, month - 1); // YOW!
	cal.set( Calendar.DATE,  day ); 
	cal.set( Calendar.HOUR_OF_DAY, hour ); 
	cal.set( Calendar.MINUTE, min );
	cal.set( Calendar.SECOND, sec );
	cal.set( Calendar.MILLISECOND, msec ); 	    

	// Still kinda unsure about all this...
	if(tzhrs != -1) {
	    cal.add( Calendar.HOUR_OF_DAY, tzhrs * mult);
	}
	if(tzmin != -1) {
	    cal.add( Calendar.MINUTE, tzmin * mult);
	}

	Date d = cal.getTime();					

	return d;
    }
}
