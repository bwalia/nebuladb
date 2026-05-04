package controllers

import (
	"k8s.io/apimachinery/pkg/util/intstr"
)

func intOrStr(s string) intstr.IntOrString { return intstr.FromString(s) }
func ptrBool(b bool) *bool                 { return &b }
func ptrInt64(i int64) *int64              { return &i }
func ptrInt32(i int32) *int32              { return &i }
