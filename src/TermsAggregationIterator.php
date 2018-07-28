<?php
namespace PhpCrudElastic;



class TermsAggregationIterator implements \Iterator
{
  protected $resultBuffer=array();
  protected $reachedEnd=false;
  protected $position = 0;
  protected $afterKey = false;
  protected $minDocCount = 1;

  public function __construct($esService, $criteria, $field)
  {
    $this->position = 0;
    $this->esService = $esService;
    $this->criteria = $criteria;
    $this->field = $field;
  }

  protected function getElasticSearchService() 
  {
    return $this->esService;
  }

  public function executeRequest()
  {
    $aggs = array(
        "my_buckets" => array(
            "composite" => array(
                "size" => 100,
                "sources" => array(
                    "item" => array(
                        "terms" => array(
                            "field" => $this->field,
                            //"min_doc_count" => $this->minDocCount
                        )
                    )
                )
            )
        )
    );

    if ($this->afterKey) {
        $aggs['my_buckets']['composite']['after'] = $this->afterKey;
    }

    $results = $this->getElasticSearchService()->aggregatePassThroughDirectly($this->criteria, $aggs);
    
    $this->afterKey = $results['my_buckets']['after_key'] ?? $this->afterKey;
    
    $itemIds = array_map(function($item){
      return $item['key']['item'];
    }, $results['my_buckets']['buckets']);

    return $itemIds;

  }
  
  public function setMinDocCount($val)
  {
    $this->minDocCount = $val;
  }
  
  public function setAfterKey($val)
  {
    $this->afterKey = $val;
  }
  
  public function getAfterKey()
  {
    return $this->afterKey;
  }
  
  
  protected function getNextBatch()
  {
    $this->resultBuffer = $this->executeRequest();

    if (count($this->resultBuffer) == 0)
    {
      $this->reachedEnd = true;
    }
    else 
    {
      $this->reachedEnd = false;
      $this->position = 0;

    }
  }
  
  public function rewind() 
  {
    $this->position=0;
    $this->resultBuffer = array();
    $this->reachedEnd = false;
    $this->getNextBatch();  
  }
  
  
  public function next()
  {
    $this->ensureReady();
    $this->position++;
    if (!isset($this->resultBuffer[$this->position]))
    {
      $this->getNextBatch();
    }
    // interestingly enough next() does not have to return the value?! There will always be a call to current?
    // yes, and there will be a call to valid. and only if it is valid, there will be a call to current.
    // return $this->resultBuffer[$this->position];
  }
  
  public function current()
  {
    $this->ensureReady();
    return $this->resultBuffer[$this->position];
  }

  public function key()
  {
    $this->ensureReady();
    return $this->position;
  }
  
  public function valid()
  {
    $this->ensureReady();
    return (isset($this->resultBuffer[$this->position]) && ($this->resultBuffer[$this->position] != false));  
  }
  
  public function hasNext()
  {
    $this->ensureReady();
    return !$this->isDone();  
  }
  
  public function isDone()
  {
    $this->ensureReady();
    return $this->reachedEnd;
  }
  
  protected function ensureReady()
  {
    if (count($this->resultBuffer) == 0)
    {
      $this->getNextBatch();
    }
    
  }

  
}




