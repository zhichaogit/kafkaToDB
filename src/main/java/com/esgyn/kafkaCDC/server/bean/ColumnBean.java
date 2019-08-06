package com.esgyn.kafkaCDC.server.bean;

import lombok.Getter;
import lombok.Setter;

/**
 *  column object for json conf
 */
public class ColumnBean {
	@Setter
    @Getter
    private String name     = null;
	@Setter
    @Getter
    private String typename = null;
	@Setter
    @Getter
	private String coltype  = null;
	@Setter
    @Getter
    private String charset  = null;
	@Setter
    @Getter
	private int    colSize  = 0;
	@Setter
    @Getter
	private int    colId    = 0;
}
