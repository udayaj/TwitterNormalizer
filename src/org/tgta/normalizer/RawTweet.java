/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.tgta.normalizer;

import java.util.Date;

/**
 *
 * @author udaya
 */
public class RawTweet {
    
    public RawTweet(){}
    
    public long Id;
    public long TwitterId;
    public String MsgText;
    public String Source;
    public float Longitude;
    public float Latitude;
    public Date CreatedTime;
    public String PlaceCountry;
    public String PlaceCountryCode;
    public String PlaceTwitterId;
    public String PlaceName;
    public String PlaceType;
    public String PlacePolygon;
    public int RetweetCount;
    public String UserLocation;
    public String PlaceFullName;
    public boolean IsNormalized;
    public boolean IsProcessed;
    
}
