package io.openshift.booster;

public class Rot13 {
    
    public static String rotate(String textToRotate) {
        if (textToRotate == null) return null;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < textToRotate.length(); i++) {
            sb.append(rotChar(textToRotate.charAt(i)));
        }
        return sb.toString();
    }

    public static char rotChar(char c) {
        if (c >= 'a' && c <= 'm') c += 13;
        else if (c >= 'A' && c <= 'M') c += 13;
        else if (c >= 'n' && c <= 'z') c -= 13;
        else if (c >= 'N' && c <= 'Z') c -= 13;
        return c;
    }
}
