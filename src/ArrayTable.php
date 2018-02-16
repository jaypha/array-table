<?php
//----------------------------------------------------------------------------
// Several functions for table arrays, include database style joins.
//----------------------------------------------------------------------------

namespace Jaypha;

//----------------------------------------------------------------------------
// These functions are exclusively for arrays types.
//----------------------------------------------------------------------------

function extract_column($table, $idx): array
{
  assert(is_array($table) || $table instanceof \Traversable);
  $column = [];
  foreach($table as $row)
    if (isset($row[$idx]))
      $column[] = $row[$idx];
  return $column;
}

//----------------------------------------------------------------------------

function remove_column(array &$table, $idx)
{
  foreach($table as $i => &$row)
    unset($row[$idx]);
}

//----------------------------------------------------------------------------

function left_join(array $left, array $right, $leftIdx, $rightIdx = null): array
{
  if (count($right) == 0) return $left;
  if ($rightIdx === null) $rightIdx = $leftIdx;

  $result = [];

  $tmpRight = [];
  foreach ($right as &$rightRow)
  {
    assert(is_array($rightRow));
    if (array_key_exists($rightIdx, $rightRow))
      $tmpRight[$rightRow[$rightIdx]][] = &$rightRow;
  }
  foreach ($left as $leftRow)
  {
    $i = $leftRow[$leftIdx];
    if (isset($tmpRight[$i]))
    {
      foreach ($tmpRight[$i] as &$r)
        $result[] = array_merge($leftRow, $r);
    }
    else
      $result[] = $leftRow;
  }
  return $result;
}

//----------------------------------------------------------------------------

function inner_join(array $left, array $right, $leftIdx, $rightIdx = null): array
{
  if (count($right) == 0) return [];
  if ($rightIdx === null) $rightIdx = $leftIdx;

  $result = [];

  $tmpRight = [];
  foreach ($right as &$rightRow)
  {
    assert(is_array($rightRow));
    if (array_key_exists($rightIdx, $rightRow))
      $tmpRight[$rightRow[$rightIdx]][] = &$rightRow;
  }
  foreach ($left as $leftRow)
  {
    $i = $leftRow[$leftIdx];
    if (isset($tmpRight[$i]))
    {
      foreach ($tmpRight[$i] as &$r)
        $result[] = array_merge($leftRow, $r);
    }
  }

  return $result;
}

//----------------------------------------------------------------------------
// Same as left join, but applies a filter to the row of $right before
// merging. This can be used to avoid overwrites.

function filtered_left_join(callable $filter, array $left, array $right, $leftIdx, $rightIdx = null): array
{
  if (count($right) == 0) return $left;
  if ($rightIdx === null) $rightIdx = $leftIdx;
  $result = [];

  $tmpRight = [];
  foreach ($right as &$rightRow)
  {
    assert(is_array($rightRow));
    if (array_key_exists($rightIdx, $rightRow))
      $tmpRight[$rightRow[$rightIdx]][] = &$rightRow;
  }

  foreach ($left as $leftRow)
  {
    $i = $leftRow[$leftIdx];
    if (isset($tmpRight[$i]))
    {
      foreach ($tmpRight[$i] as &$r)
        $result[] = array_merge($leftRow, array_filter($r, $filter,  ARRAY_FILTER_USE_BOTH));
    }
    else
      $result[] = $leftRow;
  }

  return $result;
}

//----------------------------------------------------------------------------
// These functions are for when $left is an instance of ArrayAccess. $right
// can be either an array or a Traversible.
//----------------------------------------------------------------------------

function extractColumn(\Traversable $table, $idx): array
{
  return extract_column($table, $idx);
/*
  $column = [];
  foreach($table as &$row)
  {
    assert(is_array($row) || $row instanceof \ArrayAccess);
    if (isset($row[$idx]))
      $column[] = $row[$idx];
  }
  return $column;
  */
}

//----------------------------------------------------------------------------

function removeColumn(\Traversable $table, $idx)
{
  assert($table instanceof \ArrayAccess);
  foreach($table as $i => $row)
  {
    if (is_array($row))
    {
      unset($row[$idx]);
      $table[$i] = $row;
    }
    else
    {
      assert($row instanceof \ArrayAccess);
      $row->offsetUnset($idx);
    }
  }
}

//----------------------------------------------------------------------------

function leftJoin(\ArrayAccess $left, $right, $leftIdx, $rightIdx = null): \ArrayAccess
{
  assert($left instanceof \Traversable);
  assert(is_array($right) || (
    $right instanceof \Countable &&
    $right instanceof \Traversable
  ));

  if (count($right) == 0) return $left;
  if ($rightIdx === null) $rightIdx = $leftIdx;

  $r = new \ReflectionClass($left);
  $result = $r->newInstanceWithoutConstructor();

  $tmpRight = [];
  foreach ($right as $rightRow)
  {
    if (is_array($rightRow))
    {
      if (array_key_exists($rightIdx, $rightRow))
        $tmpRight[$rightRow[$rightIdx]][] = $rightRow;
    }
    else
    {
      assert($rightRow instanceof \ArrayAccess);
      if ($rightRow->offsetExists($rightIdx))
        $tmpRight[$rightRow[$rightIdx]][] = $rightRow;
    }
  }

  foreach ($left as $leftRow)
  {
    assert(is_array($leftRow) || $leftRow instanceof \ArrayAccess);

    $i = $leftRow[$leftIdx];
    if (isset($tmpRight[$i]))
    {
      foreach ($tmpRight[$i] as $r)
      {
        $lr = is_object($leftRow) ? clone $leftRow : $leftRow;
        foreach ($r as $k => $v)
          $lr[$k] = $v;
        $result[] = $lr;
      }
    }
    else
      $result[] = is_object($leftRow) ? clone $leftRow : $leftRow;
  }

  return $result;
}

//----------------------------------------------------------------------------

function innerJoin(\ArrayAccess $left, $right, $leftIdx, $rightIdx = null): \ArrayAccess
{
  assert($left instanceof \Traversable);
  assert(is_array($right) || (
    $right instanceof \Countable &&
    $right instanceof \Traversable
  ));

  $r = new \ReflectionClass($left);
  $result = $r->newInstanceWithoutConstructor();

  if (count($right) != 0)
  {
    if ($rightIdx === null) $rightIdx = $leftIdx;

    $tmpRight = [];
    foreach ($right as $rightRow)
    {
      if (is_array($rightRow))
      {
        if (array_key_exists($rightIdx, $rightRow))
          $tmpRight[$rightRow[$rightIdx]][] = $rightRow;
      }
      else
      {
        assert($rightRow instanceof \ArrayAccess);
        if ($rightRow->offsetExists($rightIdx))
          $tmpRight[$rightRow[$rightIdx]][] = $rightRow;
      }
    }

    foreach ($left as $leftRow)
    {
      assert(is_array($leftRow) || $leftRow instanceof \ArrayAccess);

      $i = $leftRow[$leftIdx];
      if (isset($tmpRight[$i]))
      {
        foreach ($tmpRight[$i] as &$r)
        {
          $lr = is_object($leftRow) ? clone $leftRow : $leftRow;
          foreach ($r as $k => $v)
            $lr[$k] = $v;
          $result[] = $lr;
        }
      }
    }
  }

  return $result;
}

//----------------------------------------------------------------------------

function filteredLeftJoin(callable $filter, \ArrayAccess $left, $right, $leftIdx, $rightIdx = null): \ArrayAccess
{
  assert($left instanceof \Traversable);
  assert(is_array($right) || (
    $right instanceof \Countable &&
    $right instanceof \Traversable
  ));

  if (count($right) == 0) return $left;
  if ($rightIdx === null) $rightIdx = $leftIdx;

  $r = new \ReflectionClass($left);
  $result = $r->newInstanceWithoutConstructor();

  $tmpRight = [];
  foreach ($right as $rightRow)
  {
    if (is_array($rightRow))
    {
      if (array_key_exists($rightIdx, $rightRow))
        $tmpRight[$rightRow[$rightIdx]][] = $rightRow;
    }
    else
    {
      assert($rightRow instanceof \ArrayAccess);
      if ($rightRow->offsetExists($rightIdx))
        $tmpRight[$rightRow[$rightIdx]][] = $rightRow;
    }
  }

  foreach ($left as $leftRow)
  {
    assert(is_array($leftRow) || $leftRow instanceof \ArrayAccess);

    $i = $leftRow[$leftIdx];
    if (isset($tmpRight[$i]))
    {
      foreach ($tmpRight[$i] as &$r)
      {
        $lr = is_object($leftRow) ? clone $leftRow : $leftRow;
        foreach ($r as $k => $v)
          if ($filter($v, $k))
            $lr[$k] = $v;
        $result[] = $lr;
      }
    }
    else
      $result[] = is_object($leftRow) ? clone $leftRow : $leftRow;
  }

  return $result;
}

//----------------------------------------------------------------------------

trait JoinTrait
{
  function extractColumn($k): array { return extractColumn($this,$k); }
  function removeColumn($k) { removeColumn($this,$k); }

  function leftJoin($right, $leftIdx, $rightIdx = null): \ArrayAccess
  {
    return leftJoin($this, $right, $leftIdx, $rightIdx);
  }

  function innerJoin($right, $leftIdx, $rightIdx = null): \ArrayAccess
  {
    return innerJoin($this, $right, $leftIdx, $rightIdx);
  }

  function filteredLeftJoin(callable $filter, $right, $leftIdx, $rightIdx = null): \ArrayAccess
  {
    return filteredLeftJoin($filter, $this, $right, $leftIdx, $rightIdx);
  }
}

//----------------------------------------------------------------------------

class ArrayTable extends \ArrayObject
{
  use JoinTrait;
}

//----------------------------------------------------------------------------
// Copyright (C) 2017 Jaypha.
// License: BSL-1.0
// Author: Jason den Dulk
//
