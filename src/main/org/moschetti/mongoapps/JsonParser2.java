/*******************************************************************************
 * Copyright (c) 2013 EclipseSource.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 *
 * Contributors:
 *    Ralf Sternberg - initial implementation and API
 *
 * Modified to support mongodb, specifically grooming Maps  -Buzz
 ******************************************************************************/

// Here's the "mongo" dependency.  Not an additional external dep 
// because the mongo Java driver jar ALSO contains the org.bson.* 
// classes.  Whew....
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Calendar;
import java.util.Date;


public class JsonParser2 {

  private static final int MIN_BUFFER_SIZE = 10;
  private static final int DEFAULT_BUFFER_SIZE = 1024;

  private final Reader reader;
  private final char[] buffer;
  private int bufferOffset;
  private int index;
  private int fill;
  private int line;
  private int lineOffset;
  private int current;
  private StringBuilder captureBuffer;
  private int captureStart;

  private boolean useMongo;

  /*
   * |                      bufferOffset
   *                        v
   * [a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t]        < input
   *                       [l|m|n|o|p|q|r|s|t|?|?]    < buffer
   *                          ^               ^
   *                       |  index           fill
   */

  public JsonParser2( String string ) {
    this( new StringReader( string ),
          Math.max( MIN_BUFFER_SIZE, Math.min( DEFAULT_BUFFER_SIZE, string.length() ) ) );
  }

  public JsonParser2( Reader reader ) {
    this( reader, DEFAULT_BUFFER_SIZE );
  }

  public JsonParser2( Reader reader, int buffersize ) {
    this.reader = reader;
    buffer = new char[ buffersize ];
    line = 1;
    captureStart = -1;
  }


  public Object parse() throws IOException {
      return parse(false);
  }

  public Object parse(boolean observeMongoDBConventions) throws IOException {
      this.useMongo = observeMongoDBConventions;
    read();
    skipWhiteSpace();
    Object result = readValue();
    skipWhiteSpace();
    if( !isEndOfText() ) {
      throw error( "Unexpected character" );
    }
    return result;
  }


  private Object readValue() throws IOException {
    switch( current ) {
    case 'n':
      return readNull();
    case 't':
      return readTrue();
    case 'f':
      return readFalse();
    case '"':
      return readString();
    case '[':
      return readArray();
    case '{':
      return readObject();
    case '-':
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
      return readNumber();
    default:
      throw expected( "value" );
    }
  }

  private Object readArray() throws IOException {
    read();
    List array = new ArrayList();
    skipWhiteSpace();
    if( readChar( ']' ) ) {
      return array;
    }
    do {
      skipWhiteSpace();
      array.add( readValue() );
      skipWhiteSpace();
    } while( readChar( ',' ) );
    if( !readChar( ']' ) ) {
      throw expected( "',' or ']'" );
    }
    return array;
  }

  private Object readObject() throws IOException {
    read();
    Map object = new HashMap();
    Object result = null;

    skipWhiteSpace();
    if( readChar( '}' ) ) {
	result = groomMongo(object);
      return result;
    }
    do {
      skipWhiteSpace();
      String name = readName();
      skipWhiteSpace();
      if( !readChar( ':' ) ) {
        throw expected( "':'" );
      }
      skipWhiteSpace();
      object.put( name, readValue() );
      skipWhiteSpace();
    } while( readChar( ',' ) );
    if( !readChar( '}' ) ) {
      throw expected( "',' or '}'" );
    }

    result = groomMongo(object);
    return result;
  }


    private Object groomMongo(Map m) {
	Object result = m; // turn it around

	if(this.useMongo == true) {
	    int n = m.size();
	    if(n == 1) {
		Object o = null;
		if((o = m.get("$date")) != null) {
		    if(o instanceof String) {
			result = Utils.cvtISODateToDate((String)o);
			
			//result = new java.util.Date();
		    } else if(o instanceof Long) {
			Long o3 = (Long)o;
			result = new java.util.Date(o3.longValue());
		    } else if(o instanceof Integer) {
			Integer o3 = (Integer)o;
			result = new java.util.Date(o3.intValue());
		    }

		} else if((o = m.get("$numberLong")) != null) {
		    if(o instanceof String) {
			result = Long.parseLong((String)o);
		    } else {
			result = (Long)o;
		    }

		} else if((o = m.get("$oid")) != null) {
		    //byte[] bb = decodeBase64((String)o);
		    //System.out.println("$oid decodes len " + bb.length);
		    //result = new org.bson.types.ObjectId(bb);

		    // oooooooooo!  External dependency...
		    //result = new org.bson.types.ObjectId((String)o);
		    result = new ObjectId((String)o);
		}

	    } else if(n == 2) {
		Object o = null;
		if((o = m.get("$binary")) != null) {
		    String type = (String) m.get("$type");
		    if(type.equals("00")) {
			// Data in o is base64 encoded String
			byte[] bb = decodeBase64((String)o);
			result = bb;
		    }
		}
	    }
	}

	return result;
    }

  private String readName() throws IOException {
    if( current != '"' ) {
      throw expected( "name" );
    }
    return readStringInternal();
  }


  private Object readNull() throws IOException {
    read();
    readRequiredChar( 'u' );
    readRequiredChar( 'l' );
    readRequiredChar( 'l' );
    //return JsonValue.NULL;
    return null;
  }

  private Object readTrue() throws IOException {
    read();
    readRequiredChar( 'r' );
    readRequiredChar( 'u' );
    readRequiredChar( 'e' );
    return new Boolean(true);
  }

  private Object readFalse() throws IOException {
    read();
    readRequiredChar( 'a' );
    readRequiredChar( 'l' );
    readRequiredChar( 's' );
    readRequiredChar( 'e' );
    return new Boolean(false);
  }

  private void readRequiredChar( char ch ) throws IOException {
    if( !readChar( ch ) ) {
      throw expected( "'" + ch + "'" );
    }
  }

  private Object readString() throws IOException {
      return readStringInternal();
  }

  private String readStringInternal() throws IOException {
    read();
    startCapture();
    while( current != '"' ) {
      if( current == '\\' ) {
        pauseCapture();
        readEscape();
        startCapture();
      } else if( current < 0x20 ) {
        throw expected( "valid string character" );
      } else {
        read();
      }
    }
    String string = endCapture();
    read();
    return string;
  }

  private void readEscape() throws IOException {
    read();
    switch( current ) {
    case '"':
    case '/':
    case '\\':
      captureBuffer.append( (char)current );
      break;
    case 'b':
      captureBuffer.append( '\b' );
      break;
    case 'f':
      captureBuffer.append( '\f' );
      break;
    case 'n':
      captureBuffer.append( '\n' );
      break;
    case 'r':
      captureBuffer.append( '\r' );
      break;
    case 't':
      captureBuffer.append( '\t' );
      break;
    case 'u':
      char[] hexChars = new char[4];
      for( int i = 0; i < 4; i++ ) {
        read();
        if( !isHexDigit() ) {
          throw expected( "hexadecimal digit" );
        }
        hexChars[i] = (char)current;
      }
      captureBuffer.append( (char)Integer.parseInt( String.valueOf( hexChars ), 16 ) );
      break;
    default:
      throw expected( "valid escape sequence" );
    }
    read();
  }

  private Object readNumber() throws IOException {
    startCapture();
    readChar( '-' );
    int firstDigit = current;
    if( !readDigit() ) {
      throw expected( "digit" );
    }
    if( firstDigit != '0' ) {
      while( readDigit() ) {
      }
    }

    boolean hasFraction = readFraction();
    boolean hasExpo = readExponent();

    String ns = endCapture();

    Object o = null;
    if(hasFraction) {
	o = new Double(Double.parseDouble( ns ) );
    } else {
	long qq = Long.parseLong( ns );
	if(qq <= (long)Integer.MAX_VALUE) { 
	    o = new Integer(Integer.parseInt( ns ) );
	} else {
	    o = new Long(qq);
	}
    }
    return o;
  }


  private boolean readFraction() throws IOException {
    if( !readChar( '.' ) ) {
      return false;
    }
    if( !readDigit() ) {
      throw expected( "digit" );
    }
    while( readDigit() ) {
    }
    return true;
  }


  private boolean readExponent() throws IOException {
    if( !readChar( 'e' ) && !readChar( 'E' ) ) {
      return false;
    }
    if( !readChar( '+' ) ) {
      readChar( '-' );
    }
    if( !readDigit() ) {
      throw expected( "digit" );
    }
    while( readDigit() ) {
    }
    return true;
  }


  private boolean readChar( char ch ) throws IOException {
    if( current != ch ) {
      return false;
    }
    read();
    return true;
  }

  private boolean readDigit() throws IOException {
    if( !isDigit() ) {
      return false;
    }
    read();
    return true;
  }

  private void skipWhiteSpace() throws IOException {
    while( isWhiteSpace() ) {
      read();
    }
  }

  private void read() throws IOException {
    if( isEndOfText() ) {
      throw error( "Unexpected end of input" );
    }
    if( index == fill ) {
      if( captureStart != -1 ) {
        captureBuffer.append( buffer, captureStart, fill - captureStart );
        captureStart = 0;
      }
      bufferOffset += fill;
      fill = reader.read( buffer, 0, buffer.length );
      index = 0;
      if( fill == -1 ) {
        current = -1;
        return;
      }
    }
    if( current == '\n' ) {
      line++;
      lineOffset = bufferOffset + index;
    }
    current = buffer[index++];
  }

  private void startCapture() {
    if( captureBuffer == null ) {
      captureBuffer = new StringBuilder();
    }
    captureStart = index - 1;
  }

  private void pauseCapture() {
    int end = current == -1 ? index : index - 1;
    captureBuffer.append( buffer, captureStart, end - captureStart );
    captureStart = -1;
  }

  private String endCapture() {
    int end = current == -1 ? index : index - 1;
    String captured;
    if( captureBuffer.length() > 0 ) {
      captureBuffer.append( buffer, captureStart, end - captureStart );
      captured = captureBuffer.toString();
      captureBuffer.setLength( 0 );
    } else {
      captured = new String( buffer, captureStart, end - captureStart );
    }
    captureStart = -1;
    return captured;
  }

  private ParseException expected( String expected ) {
    if( isEndOfText() ) {
      return error( "Unexpected end of input" );
    }
    return error( "Expected " + expected );
  }

  private ParseException error( String message ) {
    int absIndex = bufferOffset + index;
    int column = absIndex - lineOffset;
    int offset = isEndOfText() ? absIndex : absIndex - 1;
    return new ParseException( message, offset, line, column - 1 );
  }

  private boolean isWhiteSpace() {
    return current == ' ' || current == '\t' || current == '\n' || current == '\r';
  }

  private boolean isDigit() {
    return current >= '0' && current <= '9';
  }

  private boolean isHexDigit() {
    return current >= '0' && current <= '9'
        || current >= 'a' && current <= 'f'
        || current >= 'A' && current <= 'F';
  }

  private boolean isEndOfText() {
    return current == -1;
  }


    /****
	 Base64 encode/decode
    ****/

    private final static char[] ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();

    private static int[]  toInt   = new int[128];

    static {
	for(int i=0; i< ALPHABET.length; i++){
	    toInt[ALPHABET[i]]= i;
	}
    }

    /**
     * Translates the specified byte array into Base64 string.
     *
     * @param buf the byte array (not null)
     * @return the translated Base64 string (not null)
     */
    public static String encodeBase64(byte[] buf){
	int size = buf.length;
	char[] ar = new char[((size + 2) / 3) * 4];
	int a = 0;
	int i=0;
	while(i < size){
	    byte b0 = buf[i++];
	    byte b1 = (i < size) ? buf[i++] : 0;
	    byte b2 = (i < size) ? buf[i++] : 0;
	    
	    int mask = 0x3F;
	    ar[a++] = ALPHABET[(b0 >> 2) & mask];
	    ar[a++] = ALPHABET[((b0 << 4) | ((b1 & 0xFF) >> 4)) & mask];
	    ar[a++] = ALPHABET[((b1 << 2) | ((b2 & 0xFF) >> 6)) & mask];
	    ar[a++] = ALPHABET[b2 & mask];
	}
	switch(size % 3){
	case 1: ar[--a]  = '=';
	case 2: ar[--a]  = '=';
	}
	return new String(ar);
    }
    
    /**
     * Translates the specified Base64 string into a byte array.
     *
     * @param s the Base64 string (not null)
     * @return the byte array (not null)
     */
    public static byte[] decodeBase64(String s){
	int delta = s.endsWith( "==" ) ? 2 : s.endsWith( "=" ) ? 1 : 0;
	byte[] buffer = new byte[s.length()*3/4 - delta];
	int mask = 0xFF;
	int index = 0;
	for(int i=0; i< s.length(); i+=4){
	    int c0 = toInt[s.charAt( i )];
	    int c1 = toInt[s.charAt( i + 1)];
	    buffer[index++]= (byte)(((c0 << 2) | (c1 >> 4)) & mask);
	    if(index >= buffer.length){
		return buffer;
	    }
	    int c2 = toInt[s.charAt( i + 2)];
	    buffer[index++]= (byte)(((c1 << 4) | (c2 >> 2)) & mask);
	    if(index >= buffer.length){
		return buffer;
	    }
	    int c3 = toInt[s.charAt( i + 3 )];
	    buffer[index++]= (byte)(((c2 << 6) | c3) & mask);
	}
	return buffer;
    }


}


