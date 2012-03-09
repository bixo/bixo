/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

import org.apache.log4j.Logger;

import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.utils.DomainNames;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;

import com.bixolabs.cascading.LoggingFlowProcess;
import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class AnalyzeHtmlFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    
    private static final Logger LOGGER = Logger.getLogger(AnalyzeHtmlFunction.class);

    private LoggingFlowProcess _flowProcess;
    private String _analyzerName = null;
    private transient BasePageAnalyzer _analyzer = null;
    
    public AnalyzeHtmlFunction(String analyzerName) {
        super(AnalyzedDatum.FIELDS); 
        _analyzerName = analyzerName;
    }
    
    protected static BasePageAnalyzer getAnalyzer(String analyzerName) {
        Class<?> analyzerClass;
        try {
            analyzerClass = Class.forName(analyzerName);
            return (BasePageAnalyzer)analyzerClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Unable to load class " + analyzerName, e);
        } 
    }
    
    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting analysis of html");
        _flowProcess = new LoggingFlowProcess((HadoopFlowProcess) process);
        _analyzer = getAnalyzer(_analyzerName);
    }
    
    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Ending analysis of html");
        _flowProcess.dumpCounters();
        
        super.cleanup(process, operationCall);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
        
        ParsedDatum parsedDatum = new ParsedDatum(functionCall.getArguments());
        
        String url = parsedDatum.getUrl();
        String rawHtml = parsedDatum.getParsedText();
        String hostName = DomainNames.safeGetHost(url);
        AnalyzedDatum outputDatum;
        
        try {
            _analyzer.reset(hostName, url, rawHtml, parsedDatum.getOutlinks());
            PageResult[] results = _analyzer.getResults();
            Outlink[] links = _analyzer.getLinks();
            float score = _analyzer.getScore();

            outputDatum = new AnalyzedDatum(url, score, results, links);
        } catch (Exception e) {
            LOGGER.error("Failure analyzing page: " + url, e);
            PageResult errorResult = new PageResult(url, "", "Failure analyzing page: " + e.getMessage());
            outputDatum = new AnalyzedDatum(url, 0.0f, new PageResult[] {errorResult}, new Outlink[0]);
        }
        
        functionCall.getOutputCollector().add(outputDatum.getTuple());
    }
    
    /*
     *  Convert the Bixo Outlink to our own Outlink which is serializable (since we 
     *  save the outlinks in the AnalyzedDatum).
     */
    private Outlink[] convertOutlinks(bixo.datum.Outlink[] parsedOutlinks) {
        Outlink[] result = new Outlink[parsedOutlinks.length];
        
        for (int i = 0; i < parsedOutlinks.length; i++) {
            bixo.datum.Outlink parsedOutlink = parsedOutlinks[i];
            result[i] = new Outlink(parsedOutlink.getToUrl(), parsedOutlink.getAnchor(), parsedOutlink.getRelAttributes());
        }
        
        return result;
    }
}

