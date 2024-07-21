package io.kcache.kwack.sqlline;

import sqlline.PromptHandler;
import sqlline.SqlLine;

public class KwackPromptHandler extends PromptHandler {

    public KwackPromptHandler(SqlLine sqlline) {
        super(sqlline);
    }

    @Override
    protected String getDefaultPrompt(int connectionIndex, String url, String defaultPrompt) {
        if (url != null && !url.isEmpty()) {
            if (url.contains(";")) {
                url = url.substring(0, url.indexOf(";"));
            }

            if (url.contains("?")) {
                url = url.substring(0, url.indexOf("?"));
            }

            //String resultPrompt = connectionIndex + ": " + url;
            String resultPrompt = url;
            if (resultPrompt.length() > 45) {
                resultPrompt = resultPrompt.substring(0, 45);
            }

            return resultPrompt + "> ";
        } else {
            return defaultPrompt;
        }
    }
}
