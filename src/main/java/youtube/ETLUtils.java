package youtube;

public class ETLUtils {

    /**  数据清洗
     *  1、过滤不合法数据
     *  2. 去掉&符号左右两边的空格
     *  3、 \t 换成&符号
     */

    public static String getETLString(String ori){
        String[] splits = ori.split("\t");

        // 1、过滤不合法数据
        if(splits.length < 9){
            return null;
        }

        //2、去掉&符号左右两边的空格
        splits[3] = splits[3].replaceAll(" ", "");

        StringBuilder sb = new StringBuilder();
        //3、 \t 换成&符号
        for(int i = 0; i < splits.length; i++){
            sb.append(splits[i]);
            if(i < 9){
                if(i != splits.length-1){
                    sb.append("\t");
                }
            } else {
                if(i != splits.length-1){
                    sb.append("&");
                }
            }
        }
        return sb.toString();
    }
}
