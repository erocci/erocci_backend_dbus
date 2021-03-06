<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet 
    version="1.0"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"     
    xmlns:exsl="http://exslt.org/common" 
    extension-element-prefixes="exsl" >

  <xsl:output method="xml" 
	      encoding="utf-8" 
	      indent="yes" />

  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
  </xsl:template>
  
  <xsl:template match="xs:*" >
  </xsl:template>
</xsl:stylesheet>
