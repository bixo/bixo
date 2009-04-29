package bixo.util;

import cascading.tuple.Fields;

public class FieldUtil {

    public static Fields add(Fields fields, String... moreFieldNames) {
        Fields moreFields = new Fields(moreFieldNames);
        return fields.append(moreFields);
    }

    public static Fields combine(Fields fields, Fields moreFields) {
        return fields.append(moreFields);
    }

    public static Fields prefix(Fields metaDataFields) {
        Fields  fields = new Fields();
        int size = metaDataFields.size();
        for (int i = 0; i < size; i++) {
            String fieldName = (String) metaDataFields.get(i);
            fields.

        }
        return null;
    }

}
