<?php
namespace PhpCrudElastic;



class EntityIterator implements \Iterator
{
  protected $resultBuffer=array();
  protected $reachedEnd=false;
  protected $position = 0;

  public function __construct($esService, $esSpec)
  {
    $this->position = 0;
    $this->esService = $esService;
    $this->esSpec = $esSpec;
  }

  protected function getElasticSearchService() 
  {
    return $this->esService;
  }

  
  public function setQuery($query)
  {
    $this->query = $query;
  }
  
  protected function executeRequest()
  {
    $this->esSpec['offset'] = 0;
    $this->esSpec['limit'] = 5;
    
    $lastItem = end($this->resultBuffer);
    
    if ($lastItem) {
        $this->esSpec['search_after'] = $lastItem->esSort;
    }
    
    $dataHash = $this->getElasticSearchService()->searchBySpecification($this->esSpec);
    return $dataHash['entities'];      
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
    return $this->resultBuffer[$this->position]->getId();
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




