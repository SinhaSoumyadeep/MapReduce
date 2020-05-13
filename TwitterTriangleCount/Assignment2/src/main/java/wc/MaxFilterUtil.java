package wc;

public class MaxFilterUtil {

    public static final boolean check(String from, String to, String maxUser){

        Integer frUser = Integer.parseInt(from);
        Integer toUser = Integer.parseInt(to);

        if(frUser < Integer.parseInt(maxUser) && toUser < Integer.parseInt(maxUser)){
            return true;
        }
        return false;

    }
}
